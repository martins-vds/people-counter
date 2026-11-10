"""Server-rendered, single-operator portal. Start with pc-admin or Docker Compose."""

from __future__ import annotations

import argparse
import os
import re
import secrets
import threading
from pathlib import Path
from urllib.parse import urlsplit
from zoneinfo import available_timezones
from werkzeug.exceptions import SecurityError

from flask import (
    Flask, abort, flash, redirect, render_template, request, send_file, session, url_for,
)

from people_counter import backfill_operator as backfill
from people_counter.inference_profiles import PRESETS
from people_counter.manifest import ManifestPublisherError
from people_counter.portal_service import (
    LINE_FIELDS, METADATA_FIELDS, PORTAL_CATALOG_FIELDS, REPORTS, PortalService, csv_text,
)


def _codespaces_origin() -> str | None:
    if os.environ.get("CODESPACES") != "true":
        return None
    name = os.environ.get("CODESPACE_NAME", "")
    domain = os.environ.get("GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN", "")
    port = int(os.environ.get("PC_PORTAL_PORT", "8088"))
    if (
        re.fullmatch(r"[a-z0-9][a-z0-9-]*", name) is None
        or re.fullmatch(r"(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9-]+", domain) is None
        or not 1 <= port <= 65535
    ):
        raise ValueError("Invalid Codespaces port-forwarding environment")
    return f"https://{name}-{port}.{domain}"


def create_app(service: PortalService) -> Flask:
    public_origin = _codespaces_origin()
    public_address = urlsplit(public_origin) if public_origin else None
    trusted_hosts = ["localhost", "127.0.0.1", "[::1]"]
    if public_address is not None:
        trusted_hosts.append(public_address.netloc)
    app = Flask(
        __name__,
        template_folder=os.environ.get("PC_PORTAL_TEMPLATE_DIR", "portal_templates"),
        static_folder=os.environ.get("PC_PORTAL_STATIC_DIR", "portal_static"),
        static_url_path="/static",
    )
    saved = service.store.entries("secret")
    if not saved:
        service.store.put("secret", "session", {"value": secrets.token_hex(32)})
        saved = service.store.entries("secret")
    app.config.update(
        SECRET_KEY=saved[0]["value"], MAX_CONTENT_LENGTH=64 * 1024,
        SESSION_COOKIE_HTTPONLY=True, SESSION_COOKIE_SAMESITE="Strict",
        SESSION_COOKIE_SECURE=public_origin is not None,
        TRUSTED_HOSTS=trusted_hosts,
        TEMPLATES_AUTO_RELOAD=os.environ.get("PC_PORTAL_LIVE_TEMPLATES") == "1",
    )
    app.extensions["portal_service"] = service

    @app.before_request
    def protect_forms():
        session.setdefault("csrf", secrets.token_urlsafe(32))
        if request.method == "POST":
            origin = request.headers.get("Origin")
            if origin:
                parsed = urlsplit(origin)
                address = (parsed.scheme, parsed.netloc.lower())
                forwarded = (
                    (public_address.scheme, public_address.netloc)
                    if public_address is not None else None
                )
                if address not in {(request.scheme, request.host.lower()), forwarded}:
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
        query = request.args.get("q", "")
        kind = request.args.get("kind", "all")
        if kind not in {"all", "videos", "folders"}:
            raise ValueError("Select All types, Videos, or Folders")
        entries = service.source.entries(folder)
        assignments = service.footage_assignments()
        directory = service.store.entries("camera_identity")
        for entry in entries:
            entry["camera_id"] = assignments.get(entry["path"])
        visible = [
            entry for entry in entries
            if query.casefold() in entry["name"].casefold()
            and (kind == "all" or (kind == "videos" and entry["video"]) or (kind == "folders" and entry["directory"]))
        ]
        return render_template(
            "footage.html", folder=folder, entries=visible, total=len(entries),
            root=service.source.root, query=query, kind=kind,
            folder_tree=service.source.folder_tree(folder),
            breadcrumbs=[
                {"name": name, "path": Path(*Path(folder).parts[:index + 1]).as_posix()}
                for index, name in enumerate(Path(folder).parts)
            ],
            parent=Path(folder).parent.as_posix() if folder else "",
            camera_registry=directory,
            camera_names={
                item["id"]: item["display_name"] for item in directory
            },
        )

    @app.post("/footage/assign")
    def assign_footage():
        service.assign_footage(request.form.get("camera_id", ""), request.form.getlist("videos"))
        flash("Selected footage assigned to the camera. Its saved counting line is unchanged.")
        return redirect(url_for("footage", folder=request.form.get("folder", "")))

    @app.get("/folders")
    def folders():
        return {"folders": service.source.folders(request.args.get("folder", ""))}

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
        camera_id = request.args.get("camera_id", "") or service.footage_assignments().get(video, "")
        values = {}
        folder = request.args.get("folder", Path(video).parent.as_posix())
        new_configuration = request.args.get("new") == "1"
        if not config_id and camera_id and not new_configuration:
            candidates = [
                item for item in service.store.entries("camera")
                if item["catalog"]["camera_id"] == camera_id
            ]
            if len(candidates) == 1:
                config_id = candidates[0]["id"]
            elif candidates:
                flash("Choose the saved effective-dated counting-line configuration to review.")
                return redirect(url_for("camera_footage", camera_id=camera_id))
        if config_id:
            found = next((item for item in service.store.entries("camera") if item["id"] == config_id), None)
            if found is None:
                abort(404)
            values = found["catalog"]
            folder = found["folder"]
        elif camera_id:
            registered = next((
                item for item in service.store.entries("camera_identity") if item["id"] == camera_id
            ), None)
            if registered is None:
                abort(404)
            values = {"camera_id": camera_id, "location_id": registered["location_id"]}
        return render_template(
            "calibrate.html", video=video, width=width, height=height,
            folder=folder,
            config_id=config_id, values=values,
            new_configuration=new_configuration,
            scope=request.args.get("scope", "camera"), seconds=request.args.get("seconds", "0"),
            locations=service.store.locations(), camera_registry=service.store.entries("camera_identity"),
            timezones=sorted(available_timezones()),
        )

    @app.post("/calibrate")
    def save_calibration():
        flash(service.save_calibration(request.form))
        return redirect(url_for("cameras"))

    @app.route("/cameras", methods=["GET", "POST"])
    def cameras():
        if request.method == "POST":
            service.store.add_camera(
                request.form.get("display_name", ""), request.form.get("location_id", ""),
            )
            flash("Camera added. Assign footage now, then save its counting line when ready.")
            return redirect(url_for("cameras"))
        directory = service.store.entries("camera_identity")
        locations = service.store.locations()
        return render_template(
            "cameras.html", cameras=service.store.entries("camera"),
            exceptions=service.store.entries("exception"), camera_registry=directory,
            locations=locations, selected_location=request.args.get("location_id", ""),
            camera_names={row["id"]: row["display_name"] for row in directory},
            location_names={row["id"]: row["display_name"] for row in locations},
            assignments=service.footage_assignments(),
        )

    @app.get("/cameras/<camera_id>/footage")
    def camera_footage(camera_id):
        camera = next((
            item for item in service.store.entries("camera_identity") if item["id"] == camera_id
        ), None)
        if camera is None:
            abort(404)
        return render_template(
            "camera_footage.html", camera=camera,
            videos=sorted(
                video for video, owner in service.footage_assignments().items() if owner == camera_id
            ),
            configs=[
                item for item in service.store.entries("camera")
                if item["catalog"]["camera_id"] == camera_id
            ],
        )

    @app.route("/cameras/<camera_id>/metadata/<config_id>", methods=["GET", "POST"])
    def camera_metadata(camera_id, config_id):
        found = next((
            item for item in service.store.entries("camera")
            if item["id"] == config_id and item["catalog"]["camera_id"] == camera_id
        ), None)
        if found is None:
            abort(404)
        if request.method == "POST":
            service.update_camera_metadata(camera_id, config_id, request.form)
            flash("Camera metadata saved. Footage assignments and counting line are unchanged.")
            return redirect(url_for("cameras"))
        return render_template(
            "camera_metadata.html", config=found, fields=METADATA_FIELDS,
            timezones=sorted(available_timezones()),
        )

    @app.route("/locations", methods=["GET", "POST"])
    def locations():
        if request.method == "POST":
            service.store.add_location(request.form.get("display_name", ""))
            flash("Location added. Cameras can be associated with it now or later.")
            return redirect(url_for("locations"))
        return render_template(
            "locations.html", locations=service.store.locations(),
            camera_names={
                row["id"]: row["display_name"] for row in service.store.entries("camera_identity")
            },
        )

    @app.get("/cameras.csv")
    def camera_csv():
        content = csv_text(
            PORTAL_CATALOG_FIELDS, service.camera_catalog(),
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

    @app.get("/jobs/<identity>/reports/<name>")
    def report_view(identity, name):
        if service.store.job(identity) is None or name not in REPORTS:
            abort(404)
        try:
            service.report(identity, name)
        except ValueError:
            abort(404)
        try:
            page = int(request.args.get("page", "1"))
        except ValueError as error:
            raise ValueError("Report page must be a positive integer") from error
        try:
            preview = service.report_preview(identity, name, page, request.args.get("section"))
        except ValueError as error:
            return render_template(
                "report.html", identity=identity, name=name, page=page,
                preview=None, preview_error=str(error),
            ), 400
        return render_template(
            "report.html", identity=identity, name=name, page=page, preview=preview,
        )

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
            print(f"Operator portal: {_codespaces_origin() or f'http://{args.host}:{args.port}'}", flush=True)
            serve(create_app(service), host=args.host, port=args.port, threads=4)
        finally:
            stop.set()
            worker.join(timeout=5)


if __name__ == "__main__":
    main()
