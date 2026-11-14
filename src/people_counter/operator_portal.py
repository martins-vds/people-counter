"""Server-rendered, single-operator portal. Start with pc-admin or Docker Compose."""

from __future__ import annotations

import argparse
import csv
import os
import re
import secrets
import threading
from collections.abc import Mapping
from pathlib import Path
from urllib.parse import urlsplit
from zoneinfo import available_timezones
from werkzeug.exceptions import SecurityError
from werkzeug.local import LocalProxy

from flask import (
    Flask, abort, flash, g, redirect, render_template, request, send_file, session, url_for,
)
from itsdangerous import BadSignature, URLSafeSerializer

from people_counter import backfill_operator as backfill
from people_counter.csv_reports import spreadsheet_csv
from people_counter.inference_profiles import PRESETS
from people_counter.manifest import ManifestPublisherError
from people_counter.portal_service import (
    LINE_FIELDS, METADATA_FIELDS, PORTAL_CATALOG_FIELDS, REPORT_TYPES, PortalService, report_type,
)
from people_counter.portal_sources import AdlsFootageSource
from people_counter.portal_azure import PortalAzureError


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


def _calendar_capture_time(value: str) -> str:
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d{1,6})?)?", value):
        return value + "Z"
    return value


def _inference_overrides(form: Mapping[str, str]) -> dict[str, int | float]:
    return {
        key: convert(form[key]) for key, convert in (
            ("batch_size", int), ("sample_fps", float), ("detection_threshold", float),
        ) if form.get(key)
    }


def create_app(root_service: PortalService) -> Flask:
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
    saved = root_service.store.entries("secret")
    if not saved:
        root_service.store.put("secret", "session", {"value": secrets.token_hex(32)})
        saved = root_service.store.entries("secret")
    app.config.update(
        SECRET_KEY=saved[0]["value"], MAX_CONTENT_LENGTH=2 * 1024 * 1024,
        SESSION_COOKIE_HTTPONLY=True, SESSION_COOKIE_SAMESITE="Strict",
        SESSION_COOKIE_SECURE=public_origin is not None,
        TRUSTED_HOSTS=trusted_hosts,
        TEMPLATES_AUTO_RELOAD=os.environ.get("PC_PORTAL_LIVE_TEMPLATES") == "1",
    )
    app.extensions["portal_service"] = root_service
    preview_signer = URLSafeSerializer(app.config["SECRET_KEY"], salt="full-preview-approval")
    service = LocalProxy(lambda: getattr(g, "portal_service", root_service))

    @app.before_request
    def select_draft_source():
        if request.endpoint in {"health", "static", "jobs", "job", "report", "report_view", "package_zip", "identity_refresh", "identity_status"}:
            return
        identity = request.args.get("draft") or request.form.get("draft")
        g.draft_id = identity
        g.source_id = request.args.get("source_id") or request.form.get("source_id")
        if request.endpoint in {"locations", "cameras", "camera_footage", "camera_metadata", "camera_csv"}:
            return
        if request.endpoint == "package_workspace":
            return
        if g.source_id:
            g.portal_service = root_service.for_source(root_service.source_descriptor(g.source_id))
        elif identity:
            draft = root_service.draft(identity)
            g.portal_service = root_service.for_source(draft["source"])

    @app.url_defaults
    def retain_draft(endpoint, values):
        contextual = {
            "footage", "review", "folders", "frame", "assign_footage", "draft_readiness",
            "calibrate", "camera_calibration", "save_calibration", "save_recording", "save_line",
            "camera_footage", "camera_metadata", "package_workspace", "full_preview",
            "locations", "cameras",
        }
        if getattr(g, "draft_id", None) and endpoint in contextual:
            values.setdefault("draft", g.draft_id)
        elif getattr(g, "source_id", None) and endpoint in contextual:
            values.setdefault("source_id", g.source_id)

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
            "azure_identity": root_service.azure.cached_status(), "draft_id": getattr(g, "draft_id", None),
            "source_id": getattr(g, "source_id", None),
        }

    @app.errorhandler(PortalAzureError)
    def azure_error(error):
        return render_template("error.html", message=str(error)), 403

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
    def home():
        if any(key in request.args for key in ("folder", "q", "kind")):
            return footage()
        return packages()

    @app.post("/identity/refresh")
    def identity_refresh():
        destination = request.form.get("return_to", url_for("packages"))
        address = urlsplit(destination)
        if (
            not destination.startswith("/") or destination.startswith("//")
            or address.scheme or address.netloc or "\\" in destination
            or any(character in destination for character in "\r\n\t")
        ):
            abort(400, "Identity refresh must return to a local portal page")
        root_service.azure.refresh()
        if request.accept_mimetypes.best == "application/json":
            return root_service.azure.status()
        return redirect(destination)

    @app.get("/identity/status")
    def identity_status():
        return root_service.azure.status()

    @app.get("/footage")
    def footage():
        folder = request.args.get("folder", "")
        canonical = service.source.canonical(folder, directory=True)
        folder = "" if canonical == "." else canonical
        query = request.args.get("q", "")
        kind = request.args.get("kind", "videos")
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
            root=service.source.label, query=query, kind=kind,
            folder_tree=service.source.folder_tree(folder),
            breadcrumbs=[
                {"name": name, "path": Path(*Path(folder).parts[:index + 1]).as_posix()}
                for index, name in enumerate(Path(folder).parts)
            ],
            parent=Path(folder).parent.as_posix() if folder else "",
            camera_registry=directory,
            remote_source=isinstance(service.source, AdlsFootageSource),
            camera_names={
                item["id"]: item["display_name"] for item in directory
            },
        )

    @app.post("/footage/assign")
    def assign_footage():
        service.assign_footage(
            request.form.get("camera_id", ""), request.form.getlist("videos"),
            geometry_action=request.form.get("geometry_action", ""),
        )
        flash("Selected footage assigned to the camera. Its saved counting line is unchanged.")
        if request.form.get("return_to_review") == "yes":
            return back_to_review()
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

    @app.get("/review")
    def review():
        video = request.args.get("video", "")
        readiness = service.footage_readiness(video)
        metadata = readiness["metadata"]
        exception = service.video_exception(video)
        values = {**readiness["camera_values"], **{key: exception[key] for key in LINE_FIELDS if key in exception}}
        return render_template(
            "review.html", video=video, readiness=readiness, exception=exception, values=values,
            camera_values=readiness["camera_values"], config_id=readiness["config_id"],
            width=metadata.width if metadata else 0, height=metadata.height if metadata else 0,
            seconds=request.args.get("seconds", "0"),
            folder=request.args.get("folder", str(Path(video).parent)),
            return_folder=request.args.get("folder", ""), query=request.args.get("q", ""),
            kind=request.args.get("kind", "videos"), camera_registry=service.store.entries("camera_identity"),
            remote_source=isinstance(service.source, AdlsFootageSource),
        )

    @app.route("/footage/full-preview", methods=["GET", "POST"])
    def full_preview():
        if not isinstance(service.source, AdlsFootageSource):
            raise ValueError("Full-download fallback applies only to ADLS footage")
        video = request.values.get("video", "")
        if request.method == "POST":
            if request.form.get("confirm") != "yes":
                abort(403, "Approve the full download explicitly")
            try:
                reviewed = preview_signer.loads(request.form.get("approval", ""))
            except BadSignature:
                abort(400, "Review and approve this recording before downloading")
            if reviewed["source"] != service.source.descriptor or reviewed["video"] != video:
                abort(400, "The approved source or recording changed. Review the preview again.")
            if reviewed["observations"] != service.source.properties(video):
                abort(400, "Footage changed after approval. Review and approve the full preview again.")
            service.full_preview(video, 0, observations=reviewed["observations"])
            flash("Approved full preview completed. Temporary video deleted; only JPEG and verified metadata retained.")
            return redirect(url_for("review", video=video))
        properties = service.source.properties(video)
        approval = preview_signer.dumps({"source": service.source.descriptor, "video": video, "observations": properties})
        return render_template("full_preview.html", video=video, size=properties["size"], approval=approval)

    @app.get("/calibrate")
    def calibrate():
        video = request.args.get("video", "")
        service.source.properties(video)
        owner = service.footage_assignments().get(video)
        if request.args.get("scope") == "video" or not owner:
            return redirect(url_for("review", video=video, folder=request.args.get("folder", "")))
        if request.args.get("camera_id", owner) != owner:
            raise ValueError("Reference footage must already belong to this camera")
        return redirect(url_for(
            "camera_calibration", camera_id=owner, video=video,
            config_id=request.args.get("config_id", ""),
        ))

    @app.post("/footage/readiness")
    def draft_readiness():
        video = request.form.get("video", "")
        fields = ("camera_id", "geometry_action", "line_override", "captured_at_utc", *LINE_FIELDS)
        draft = {name: request.form[name] for name in fields if name in request.form}
        if "captured_at_utc" in draft:
            draft["captured_at_utc"] = _calendar_capture_time(draft["captured_at_utc"])
        readiness = service.footage_readiness(video, draft=draft)
        metadata = readiness["metadata"]
        return render_template(
            "footage_readiness.html", readiness=readiness, draft=True,
            exception=readiness["exception"], camera_values=readiness["camera_values"],
            width=metadata.width if metadata else 0, height=metadata.height if metadata else 0,
        )

    @app.route("/cameras/<camera_id>/calibrate", methods=["GET", "POST"])
    def camera_calibration(camera_id):
        camera = service.registered_camera(camera_id)
        if request.method == "POST":
            service.save_camera_line(camera_id, request.form)
            flash("Camera calibration saved. Footage ownership and video exceptions are unchanged.")
            return redirect(url_for("camera_footage", camera_id=camera_id))
        videos = sorted(video for video, owner in service.footage_assignments().items() if owner == camera_id)
        config_id = request.args.get("config_id", "")
        configs = [item for item in service.store.entries("camera") if item["catalog"]["camera_id"] == camera_id]
        if not config_id and len(configs) == 1:
            config_id = configs[0]["id"]
        config = next((item for item in configs if item["id"] == config_id), None)
        if config_id and config is None:
            abort(404)
        video = request.args.get("video", videos[0] if videos else "")
        if video and video not in videos:
            raise ValueError("Choose reference footage already associated with this camera")
        readiness = service.footage_readiness(video) if video else None
        metadata = readiness["metadata"] if readiness else None
        return render_template(
            "camera_calibration.html", camera=camera, videos=videos, video=video,
            configs=configs, config=config, config_id=config_id, readiness=readiness,
            values=config["catalog"] if config else {}, seconds=request.args.get("seconds", "0"),
            width=metadata.width if metadata else 0, height=metadata.height if metadata else 0,
        )

    @app.post("/calibrate")
    def save_calibration():
        form = request.form.to_dict()
        if form.get("scope") == "camera":
            service.save_camera_line(form.get("camera_id", ""), form)
            flash("Camera calibration saved. Footage ownership and video exceptions are unchanged.")
        else:
            form["captured_at_utc"] = _calendar_capture_time(form.get("captured_at_utc", ""))
            flash(service.save_calibration(form))
        return redirect(url_for("cameras"))

    def back_to_review():
        return redirect(url_for(
            "review", video=request.form.get("video", ""),
            folder=request.form.get("return_folder", request.form.get("folder", "")),
            q=request.form.get("q", ""), kind=request.form.get("kind", "videos"),
        ))

    @app.post("/footage/recording")
    def save_recording():
        service.update_video_exception(request.form.get("video", ""), {
            "captured_at_utc": _calendar_capture_time(request.form.get("captured_at_utc", "")),
        })
        flash("Recording details saved. Counting line unchanged. Prepare a new package to use these changes.")
        return back_to_review()

    @app.post("/footage/line")
    def save_line():
        service.save_line(request.form)
        flash("Video counting line saved. Camera calibration and recording details unchanged.")
        return back_to_review()

    @app.route("/cameras", methods=["GET", "POST"])
    def cameras():
        if request.method == "POST":
            service.store.add_camera(
                request.form.get("display_name", ""), request.form.get("location_id", ""),
            )
            flash("Camera added. Assign footage now, then calibrate the camera when ready.")
            return redirect(url_for("cameras"))
        directory = service.store.entries("camera_identity")
        locations = service.store.locations()
        return render_template(
            "cameras.html", cameras=service.store.entries("camera"),
            exceptions=service.store.entries("exception"), camera_registry=directory,
            locations=locations, selected_location=request.args.get("location_id", ""),
            camera_names={row["id"]: row["display_name"] for row in directory},
            location_names={row["id"]: row["display_name"] for row in locations},
            assignments={row["id"]: row["camera_id"] for row in root_service.store.entries("footage")},
        )

    @app.get("/cameras/<camera_id>/footage")
    def camera_footage(camera_id):
        camera = next((
            item for item in service.store.entries("camera_identity") if item["id"] == camera_id
        ), None)
        if camera is None:
            abort(404)
        videos = sorted(
            row["id"] for row in root_service.store.entries("footage") if row["camera_id"] == camera_id
        )
        contexts = {video: root_service.reference_context(video) for video in videos}
        configs = [
            item for item in service.store.entries("camera")
            if item["catalog"]["camera_id"] == camera_id
        ]
        for config in configs:
            contexts.setdefault(config["video"], root_service.reference_context(config["video"]))
        return render_template(
            "camera_footage.html", camera=camera,
            videos=videos, configs=configs, video_contexts=contexts,
            calibration_context=contexts[videos[0]] if videos else {"source_id": ""},
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
        content = spreadsheet_csv(
            PORTAL_CATALOG_FIELDS, service.camera_catalog(),
        )
        return app.response_class(
            content, mimetype="text/csv",
            headers={"Content-Disposition": 'attachment; filename="camera-catalog.csv"'},
        )

    @app.route("/packages", methods=["GET", "POST"])
    def packages():
        if request.method == "POST":
            if isinstance(service.source, AdlsFootageSource):
                raise ValueError("Use the package draft workflow to freeze ADLS selection")
            identity = service.enqueue_preparation(
                request.form.get("folder", ""), name=request.form.get("package_name", ""),
                destination=request.form.get("destination", ""),
            )
            return redirect(url_for("job", identity=identity))
        return render_template(
            "packages.html", folder=request.args.get("folder", ""),
            cameras=service.store.entries("camera"), packages=service.packages(),
            output_root=service.output_root,
            drafts=service.store.entries("draft"),
        )

    @app.post("/packages/new")
    def new_package():
        draft = root_service.new_draft(request.form.get("package_name", ""))
        return redirect(url_for("package_workspace", identity=draft["id"], step="source", draft=draft["id"]))

    @app.route("/packages/drafts/<identity>", methods=["GET", "POST"])
    def package_workspace(identity):
        draft = root_service.draft(identity)
        g.draft_id = identity
        step = request.args.get("step", "source")
        if step not in {"source", "footage", "cameras", "generate"}:
            raise ValueError("Select an existing package step")
        selecting_source = (
            request.method == "GET" and step == "source"
            or request.method == "POST" and request.form.get("action") == "source"
        )
        scoped = root_service if selecting_source else root_service.for_source(draft["source"])
        if request.method == "POST":
            step = save_package_step(identity, draft, scoped)
            if not isinstance(step, str):
                return step
            return redirect(url_for("package_workspace", identity=identity, step=step, draft=identity))
        folder = request.args.get("folder", draft["folder"])
        page = int(request.args.get("page", "1"))
        if page < 1:
            raise ValueError("Page must be positive")
        rows, next_token = [], None
        has_more = False
        if step == "footage":
            if isinstance(scoped.source, AdlsFootageSource):
                rows, next_token = scoped.source.page(folder, request.args.get("continuation"))
            else:
                entries = scoped.source.entries(folder)
                rows = entries[(page - 1) * 100:page * 100]
                has_more = len(entries) > page * 100
        subscriptions, accounts, filesystems = [], [], []
        kind = request.args.get("kind", draft["source"]["kind"])
        subscription = request.args.get("subscription", draft["source"].get("subscription", ""))
        account = request.args.get("account", draft["source"].get("account", ""))
        source_error = ""
        if step == "source" and kind == "adls":
            identity_status = (
                root_service.azure.status() if "kind" in request.args
                else root_service.azure.cached_status()
            )
            if not identity_status["signed_in"]:
                source_error = identity_status["error"]
            elif "kind" in request.args:
                try:
                    subscriptions = root_service.azure.subscriptions()
                    if subscription:
                        accounts = root_service.azure.accounts(subscription)
                    if account and subscription:
                        filesystems = root_service.azure.filesystems(account, subscription)
                except PortalAzureError as error:
                    source_error = str(error)
        selected = draft["videos"][(page - 1) * 100:page * 100]
        return render_template(
            "package_workspace.html", draft=draft, step=step, source=scoped.source,
            entries=rows, next_token=next_token, folder=folder,
            has_more=has_more,
            selected=selected, assignments=scoped.footage_assignments(),
            cameras=service.store.entries("camera_identity"), locations=service.store.locations(),
            subscriptions=subscriptions, accounts=accounts, filesystems=filesystems,
            kind=kind, subscription=subscription, account=account, page=page,
            source_error=source_error,
            selected_bytes=sum(value["size"] for value in draft["observations"].values()),
            output_root=service.output_root,
            local_root=root_service.local_source.root,
        )

    def save_package_step(identity, draft, scoped):
        action = request.form.get("action", "")
        if action == "source":
            descriptor = {"kind": request.form.get("kind", "")}
            if descriptor["kind"] == "local":
                descriptor["root"] = str(root_service.local_source.root)
            if descriptor["kind"] == "adls":
                descriptor.update({key: request.form.get(key, "") for key in ("subscription", "account", "filesystem", "root")})
                if not any(row["name"] == descriptor["account"] for row in root_service.azure.accounts(descriptor["subscription"])):
                    raise ValueError("Select an HNS-enabled account in the current tenant and subscription")
                if not any(row["name"] == descriptor["filesystem"] for row in root_service.azure.filesystems(descriptor["account"], descriptor["subscription"])):
                    raise ValueError("Select an accessible filesystem")
            if draft["videos"] and descriptor != draft["source"] and request.form.get("confirm_source") != "yes":
                raise ValueError("Confirm changing the source and clearing selected footage")
            root_service.update_draft(identity, {"source": descriptor})
            return "footage"
        if action == "selection":
            mode = request.form.get("mode")
            if mode == "individual":
                visible = set(request.form.getlist("visible"))
                videos = sorted((set(draft["videos"]) - visible) | set(request.form.getlist("videos")))
                changes = {"mode": mode, "videos": videos}
            else:
                changes = {"mode": mode, "folder": request.form.get("folder", "")}
            root_service.update_draft(identity, changes)
            return request.form.get("next_step", "footage")
        if action == "assign":
            videos = request.form.getlist("videos")
            if not set(videos).issubset(draft["videos"]):
                raise ValueError("Assign only footage selected for this package")
            scoped.assign_footage(
                request.form.get("camera_id", ""), videos,
                geometry_action=request.form.get("geometry_action", ""),
            )
            flash("Camera assignment saved. Package draft retained.")
            return "cameras"
        if action == "destination":
            root_service.update_draft(identity, {"destination": request.form.get("destination", "")})
            return "generate"
        if action == "generate":
            job = root_service.generate_draft(identity)
            return redirect(url_for("job", identity=job))
        raise ValueError("Select an existing package action")

    @app.post("/packages/<identity>/delete")
    def delete_package(identity):
        if request.form.get("confirm") != "yes":
            abort(400, "Confirm deletion of the selected package artifacts")
        service.delete_package(identity)
        flash("Selected package artifacts deleted. Source footage, reports, history, and recovery evidence retained.")
        return redirect(url_for("packages"))

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
            overrides = _inference_overrides(request.form)
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
            overrides = _inference_overrides(request.form)
            if request.form.get("preview") == "yes":
                package = service.store.job(request.form.get("package_id", ""))
                if package is None or package["kind"] != "prepare" or package["status"] != "SUCCEEDED":
                    raise ValueError("Select a successfully prepared package")
                service.package_dir(package["id"])
                if action not in {"publish", "submit"}:
                    raise ValueError("Select publication or explicit submission")
                if action == "publish" and overrides:
                    raise ValueError("Inference overrides apply only to processing submissions")
                config = backfill.load_target(service.catalog, target, profile, overrides=overrides)
                defaults = backfill.load_target(service.catalog, target, profile)
                return render_template(
                    "cloud_review.html", config=config, defaults=defaults, form=request.form,
                    overrides=overrides,
                )
            identity = service.enqueue_cloud(
                request.form.get("package_id", ""), target, profile, action, wait,
                overrides=overrides, capacity=request.form.get("capacity", ""),
            )
            return redirect(url_for("job", identity=identity))
        choices = []
        if service.catalog.exists():
            catalog = backfill._read_catalog(service.catalog)
            for target, entry in catalog["targets"].items():
                for name in entry["profiles"]:
                    config = backfill.load_target(service.catalog, target, name)
                    choices.append({"value": f"{target}::{name}", "target": target, "profile": config.profile})
        prepared = [item for item in service.packages() if item["status"] == "SUCCEEDED" and not item["deleted"]]
        return render_template(
            "cloud.html", choices=choices, packages=prepared,
            selected_package=request.args.get("package_id", ""),
            capacities=[f"F{2 ** exponent}" for exponent in range(1, 12)],
        )

    @app.get("/jobs")
    def jobs():
        return render_template("jobs.html", jobs=service.store.jobs())

    @app.get("/jobs/<identity>")
    def job(identity):
        found = service.store.job(identity)
        if found is None:
            abort(404)
        available = []
        for key, (name, label, format) in REPORT_TYPES.items():
            try:
                service.report(identity, name)
                available.append({"id": key, "name": name, "label": label, "format": format})
            except ValueError:
                continue
        summary = None
        summary_error = None
        if any(item["id"] == "summary" for item in available):
            try:
                summary = service.report_preview(identity, "summary", 1)
            except ValueError as error:
                summary_error = str(error)
        package = next((item for item in service.packages() if item["id"] == identity), None)
        return render_template(
            "job.html", job=found, events=service.store.events(identity), reports=available,
            summary=summary, summary_error=summary_error, package=package,
        )

    @app.post("/jobs/<identity>/resume")
    def resume(identity):
        found = service.store.job(identity)
        if found is None:
            abort(404)
        if found["kind"] in {"publish", "submit"}:
            cloud_confirmation()
        if found["payload"].get("execution_identity"):
            root_service.azure.credential(found["payload"]["execution_identity"]).get_token("https://storage.azure.com/.default")
        service.store.retry(identity)
        return redirect(url_for("job", identity=identity))

    @app.get("/jobs/<identity>/files/<name>")
    def report(identity, name):
        if service.store.job(identity) is None:
            abort(404)
        try:
            path = service.report(identity, name)
        except ValueError:
            abort(404)
        if path.suffix == ".csv":
            def export():
                with path.open(newline="", encoding="utf-8") as handle:
                    reader = csv.DictReader(handle)
                    yield from spreadsheet_csv(reader.fieldnames or (), reader)
            return app.response_class(
                export(), mimetype="text/csv",
                headers={"Content-Disposition": f'attachment; filename="{path.name}"'},
            )
        return send_file(path, as_attachment=True)

    @app.get("/jobs/<identity>/reports/<name>")
    def report_view(identity, name):
        if service.store.job(identity) is None:
            abort(404)
        try:
            service.report(identity, name)
            _, label, format = report_type(name)
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
                preview=None, preview_error=str(error), label=label, format=format,
            ), 400
        return render_template(
            "report.html", identity=identity, name=name, page=page, preview=preview,
            label=label, format=format,
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
    parser.add_argument("--output-root", type=Path, default=Path(os.environ["PC_PORTAL_OUTPUT_ROOT"]) if os.environ.get("PC_PORTAL_OUTPUT_ROOT") else None)
    binding = parser.add_mutually_exclusive_group()
    binding.add_argument("--host", choices=("127.0.0.1", "::1", "localhost"), default="127.0.0.1")
    binding.add_argument(
        "--container-loopback-publish", dest="host", action="store_const", const="0.0.0.0",
        help="Bind inside a container only; publish its port on host loopback, never a public interface.",
    )
    parser.add_argument("--port", type=int, default=8088)
    parser.add_argument("--allow-cloud", action="store_true", default=os.environ.get("PC_PORTAL_ALLOW_CLOUD") == "1")
    args = parser.parse_args()
    os.umask(0o077)
    service = PortalService(
        args.source_root, args.models_root, args.state_dir,
        allow_cloud=args.allow_cloud, output_root=args.output_root,
    )
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
