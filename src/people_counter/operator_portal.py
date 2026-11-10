"""Server-rendered, single-operator portal. Start with pc-admin or Docker Compose."""

from __future__ import annotations

import argparse
import os
import secrets
import threading
from pathlib import Path
from urllib.parse import urlsplit
from werkzeug.exceptions import SecurityError

from flask import (
    Flask, abort, flash, redirect, render_template, request, send_file, session, url_for,
)

from people_counter import backfill_operator as backfill
from people_counter.inference_profiles import PRESETS
from people_counter.manifest import CATALOG_COLUMNS, ManifestPublisherError
from people_counter.portal_service import LINE_FIELDS, REPORTS, PortalService, csv_text


def create_app(service: PortalService) -> Flask:
    app = Flask(
        __name__, template_folder="portal_templates", static_folder="portal_static",
        static_url_path="/static",
    )
    saved = service.store.entries("secret")
    if not saved:
        service.store.put("secret", "session", {"value": secrets.token_hex(32)})
        saved = service.store.entries("secret")
    app.config.update(
        SECRET_KEY=saved[0]["value"], MAX_CONTENT_LENGTH=64 * 1024,
        SESSION_COOKIE_HTTPONLY=True, SESSION_COOKIE_SAMESITE="Strict",
        TRUSTED_HOSTS=["localhost", "127.0.0.1", "[::1]"],
    )
    app.extensions["portal_service"] = service

    @app.before_request
    def protect_forms():
        session.setdefault("csrf", secrets.token_urlsafe(32))
        if request.method == "POST":
            origin = request.headers.get("Origin")
            if origin:
                parsed = urlsplit(origin)
                if (parsed.scheme, parsed.netloc.lower()) != (request.scheme, request.host.lower()):
                    abort(403, "Cross-origin actions are not permitted")
            if not secrets.compare_digest(request.form.get("csrf", ""), session["csrf"]):
                abort(403, "Invalid form token; reload the page and try again")

    @app.after_request
    def browser_headers(response):
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self'; style-src 'self'; "
            "img-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'"
        )
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "same-origin"
        response.headers["Cache-Control"] = "no-store"
        return response

    @app.context_processor
    def common():
        return {
            "cloud_enabled": service.allow_cloud, "line_fields": LINE_FIELDS,
            "worker_error": service.worker_error,
        }

    @app.errorhandler(ValueError)
    @app.errorhandler(ManifestPublisherError)
    def invalid_input(error):
        return render_template("error.html", message=str(error)), 400

    @app.errorhandler(400)
    @app.errorhandler(403)
    @app.errorhandler(404)
    @app.errorhandler(413)
    def http_error(error):
        if isinstance(error, SecurityError):
            return "Untrusted host", 400, {"Content-Type": "text/plain; charset=utf-8"}
        return render_template("error.html", message=error.description), error.code

    @app.get("/healthz")
    def health():
        if service.worker_error is not None:
            return {"status": "error", "error": service.worker_error}, 503
        return {"status": "ok"}

    @app.get("/")
    def footage():
        folder = request.args.get("folder", "")
        canonical = service.source.resolve(folder).relative_to(service.source.root).as_posix()
        folder = "" if canonical == "." else canonical
        return render_template(
            "footage.html", folder=folder, entries=service.source.entries(folder),
            root=service.source.root,
            parent=Path(folder).parent.as_posix() if folder else "",
        )

    @app.get("/frame")
    def frame():
        content, _, _ = service.source.frame(
            request.args.get("video", ""), float(request.args.get("seconds", "0")),
        )
        return app.response_class(content, mimetype="image/jpeg")

    @app.get("/calibrate")
    def calibrate():
        video = request.args.get("video", "")
        _, width, height = service.source.frame(
            video, float(request.args.get("seconds", "0")),
        )
        config_id = request.args.get("config_id", "")
        values = {}
        if config_id:
            found = next((item for item in service.store.entries("camera") if item["id"] == config_id), None)
            if found is None:
                abort(404)
            values = found["catalog"]
        return render_template(
            "calibrate.html", video=video, width=width, height=height,
            folder=request.args.get("folder", Path(video).parent.as_posix()),
            config_id=config_id, values=values,
            scope=request.args.get("scope", "camera"), seconds=request.args.get("seconds", "0"),
        )

    @app.post("/calibrate")
    def save_calibration():
        flash(service.save_calibration(request.form))
        return redirect(url_for("cameras"))

    @app.get("/cameras")
    def cameras():
        return render_template(
            "cameras.html", cameras=service.store.entries("camera"),
            exceptions=service.store.entries("exception"),
        )

    @app.get("/cameras.csv")
    def camera_csv():
        content = csv_text(
            tuple(sorted(CATALOG_COLUMNS)), [item["catalog"] for item in service.store.entries("camera")],
        )
        return app.response_class(
            content, mimetype="text/csv",
            headers={"Content-Disposition": 'attachment; filename="camera-catalog.csv"'},
        )

    @app.route("/packages", methods=["GET", "POST"])
    def packages():
        if request.method == "POST":
            identity = service.enqueue_preparation(request.form.get("folder", ""))
            return redirect(url_for("job", identity=identity))
        return render_template(
            "packages.html", folder=request.args.get("folder", ""),
            cameras=service.store.entries("camera"),
        )

    @app.route("/profiles", methods=["GET", "POST"])
    def profiles():
        if request.method == "POST":
            selected = request.form.getlist("profiles")
            if not selected or any(name not in PRESETS for name in selected):
                raise ValueError("Select at least one supported profile")
            fields = (
                "workspace_id", "lakehouse_id", "environment_id", "control_sjd_id",
                "storage_account", "filesystem", "shortcut_path", "table_directory",
            )
            binding = {name: request.form.get(name, "") for name in fields}
            binding["intake_mode"] = "explicit"
            overrides = {}
            if request.form.get("batch_size"):
                overrides["batch_size"] = int(request.form["batch_size"])
            if request.form.get("sample_fps"):
                overrides["sample_fps"] = float(request.form["sample_fps"])
            identity = service.enqueue("profiles", {
                "target": request.form.get("target", ""), "binding": binding,
                "profiles": selected, "overrides": overrides,
            })
            return redirect(url_for("job", identity=identity))
        catalog = backfill._read_catalog(service.catalog) if service.catalog.exists() else {"targets": {}}
        return render_template("profiles.html", presets=sorted(PRESETS), catalog=catalog, models=service.models_root)

    def cloud_confirmation():
        if not service.allow_cloud:
            abort(403, "Cloud actions are disabled. Enable them explicitly when ready.")
        if request.form.get("confirm") != "yes":
            abort(403, "Explicit confirmation is required for cloud actions")

    @app.route("/cloud", methods=["GET", "POST"])
    def cloud():
        if request.method == "POST":
            cloud_confirmation()
            target, separator, profile = request.form.get("selection", "").partition("::")
            if not separator:
                raise ValueError("Select an approved target and profile")
            action = request.form.get("action", "")
            wait = request.form.get("wait") == "yes"
            if wait and action != "submit":
                raise ValueError("Waiting for processing requires explicit submission")
            identity = service.enqueue_cloud(
                request.form.get("package_id", ""), target, profile, action, wait,
            )
            return redirect(url_for("job", identity=identity))
        choices = []
        if service.catalog.exists():
            catalog = backfill._read_catalog(service.catalog)
            for target, entry in catalog["targets"].items():
                for name in entry["profiles"]:
                    config = backfill.load_target(service.catalog, target, name)
                    choices.append({"value": f"{target}::{name}", "target": target, "profile": config.profile})
        prepared = [
            item for item in service.store.jobs()
            if item["kind"] == "prepare" and item["status"] == "SUCCEEDED"
        ]
        return render_template("cloud.html", choices=choices, packages=prepared)

    @app.get("/jobs")
    def jobs():
        return render_template("jobs.html", jobs=service.store.jobs())

    @app.get("/jobs/<identity>")
    def job(identity):
        found = service.store.job(identity)
        if found is None:
            abort(404)
        available = []
        for name in REPORTS:
            try:
                service.report(identity, name)
                available.append(name)
            except ValueError:
                continue
        return render_template("job.html", job=found, events=service.store.events(identity), reports=available)

    @app.post("/jobs/<identity>/resume")
    def resume(identity):
        found = service.store.job(identity)
        if found is None:
            abort(404)
        if found["kind"] in {"publish", "submit"}:
            cloud_confirmation()
        service.store.retry(identity)
        return redirect(url_for("job", identity=identity))

    @app.get("/jobs/<identity>/files/<name>")
    def report(identity, name):
        if service.store.job(identity) is None or name not in REPORTS:
            abort(404)
        try:
            path = service.report(identity, name)
        except ValueError:
            abort(404)
        return send_file(path, as_attachment=True)

    @app.get("/jobs/<identity>/package.zip")
    def package_zip(identity):
        if service.store.job(identity) is None:
            abort(404)
        return send_file(service.archive(identity), as_attachment=True)

    return app


def main() -> None:
    from filelock import FileLock
    from waitress import serve

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=Path(os.environ.get("PC_PORTAL_SOURCE_ROOT", "samples")))
    parser.add_argument("--models-root", type=Path, default=Path(os.environ.get("PC_PORTAL_MODELS_ROOT", "offline-models")))
    parser.add_argument("--state-dir", type=Path, default=Path(os.environ.get("PC_PORTAL_STATE_DIR", ".people-counter-portal")))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8088)
    parser.add_argument("--allow-cloud", action="store_true", default=os.environ.get("PC_PORTAL_ALLOW_CLOUD") == "1")
    args = parser.parse_args()
    os.umask(0o077)
    service = PortalService(args.source_root, args.models_root, args.state_dir, allow_cloud=args.allow_cloud)
    with FileLock(service.state / "server.lock", timeout=0):
        stop = threading.Event()
        worker = threading.Thread(target=service.work, args=(stop,), daemon=True)
        worker.start()
        try:
            print(f"Operator portal: http://{args.host}:{args.port}", flush=True)
            serve(create_app(service), host=args.host, port=args.port, threads=4)
        finally:
            stop.set()
            worker.join(timeout=5)


if __name__ == "__main__":
    main()
