"use strict";

const identityDisclosure = document.querySelector("[data-azure-identity]");
if (identityDisclosure) {
  const message = identityDisclosure.querySelector("[data-identity-error]");
  let generation = 0;
  const displayIdentity = identity => {
    const fields = ["name", "initials", "account", "tenant_id", "error"];
    if (typeof identity.signed_in !== "boolean" || !fields.every(name => typeof identity[name] === "string")) {
      throw new Error("Unexpected identity response");
    }
    for (const name of fields) {
      const attribute = name === "tenant_id" ? "tenant" : name;
      identityDisclosure.querySelector(`[data-identity-${attribute}]`).textContent = identity[name];
    }
  };
  const updateIdentity = async (url, options) => {
    const current = ++generation;
    identityDisclosure.setAttribute("aria-busy", "true");
    message.textContent = "Checking Azure CLI identity...";
    try {
      const response = options ? await fetch(url, options) : await fetch(url);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const identity = await response.json();
      if (current !== generation) return;
      displayIdentity(identity);
    } catch (error) {
      if (current === generation) message.textContent = `Unable to refresh Azure CLI identity: ${error.message}. Retry with Refresh.`;
    } finally {
      if (current === generation) identityDisclosure.setAttribute("aria-busy", "false");
    }
  };
  identityDisclosure.addEventListener("focusout", event => {
    if (!identityDisclosure.contains(event.relatedTarget)) identityDisclosure.open = false;
  });
  identityDisclosure.addEventListener("keydown", event => {
    if (event.key === "Escape" && identityDisclosure.open && !event.defaultPrevented && !event.isComposing) {
      event.preventDefault();
      identityDisclosure.open = false;
      identityDisclosure.querySelector("summary").focus();
    }
  });
  const form = identityDisclosure.querySelector("[data-identity-refresh]");
  form.addEventListener("submit", event => {
    event.preventDefault();
    updateIdentity(form.action, { method: "POST", headers: { Accept: "application/json" }, body: new FormData(form) });
  });
  updateIdentity(identityDisclosure.dataset.statusUrl);
}

document.addEventListener("invalid", (event) => {
  for (let section = event.target.closest("details"); section; section = section.parentElement.closest("details")) {
    section.open = true;
  }
}, true);

const folderNavigation = document.querySelector("#folder-navigation");
if (folderNavigation) {
  const sidebar = document.querySelector("#folder-sidebar");
  const toggle = document.querySelector("#toggle-folders");
  toggle.addEventListener("click", () => {
    const expanded = toggle.getAttribute("aria-expanded") !== "true";
    toggle.setAttribute("aria-expanded", String(expanded));
    sidebar.classList.toggle("hidden", !expanded);
  });
  const folderIcon = folderNavigation.querySelector(".folder-link svg");
  const message = (text) => {
    const item = document.createElement("li");
    item.className = "folder-message";
    item.textContent = text;
    return item;
  };
  const bindFolder = (node) => {
    node.addEventListener("toggle", () => { if (node.open) loadFolders(node); });
  };
  const makeFolder = (folder) => {
    const item = document.createElement("li");
    const node = document.createElement("details");
    node.className = "folder-node";
    node.dataset.folder = folder.path;
    const summary = document.createElement("summary");
    const link = document.createElement("a");
    const url = new URL(folderNavigation.dataset.browseUrl, location.origin);
    url.searchParams.set("folder", folder.path);
    url.searchParams.set("q", folderNavigation.dataset.query);
    url.searchParams.set("kind", folderNavigation.dataset.kind);
    link.href = url.toString();
    link.className = "folder-link";
    if (folder.path === folderNavigation.dataset.currentFolder) link.setAttribute("aria-current", "page");
    const label = document.createElement("span");
    label.textContent = folder.name;
    link.append(folderIcon.cloneNode(true), label);
    summary.append(link);
    const children = document.createElement("ul");
    children.dataset.folderChildren = "";
    node.append(summary, children);
    item.append(node);
    bindFolder(node);
    return item;
  };
  const loadFolders = async (node) => {
    if (node.dataset.loaded === "true" || node.dataset.loading === "true") return;
    const children = node.querySelector(":scope > [data-folder-children]");
    node.dataset.loading = "true";
    const loading = message("Loading folders...");
    loading.setAttribute("role", "status");
    children.replaceChildren(loading);
    let folders;
    try {
      const url = new URL(folderNavigation.dataset.foldersUrl, location.origin);
      url.searchParams.set("folder", node.dataset.folder);
      const response = await fetch(url);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const body = await response.json();
      if (!Array.isArray(body.folders) || !body.folders.every(folder => typeof folder.name === "string" && typeof folder.path === "string")) {
        throw new Error("Unexpected folder response");
      }
      folders = body.folders;
    } catch (error) {
      const failure = message(`Unable to load folders: ${error.message}`);
      failure.setAttribute("role", "alert");
      const retry = document.createElement("button");
      retry.type = "button";
      retry.className = "btn-secondary btn-sm";
      retry.textContent = "Retry";
      retry.setAttribute("aria-label", `Retry folder ${node.dataset.folder}`);
      retry.addEventListener("click", async () => {
        await loadFolders(node);
        node.querySelector(":scope > summary").focus();
      });
      failure.append(document.createElement("br"), retry);
      children.replaceChildren(failure);
      return;
    } finally {
      node.dataset.loading = "false";
    }
    children.replaceChildren(...(folders.length ? folders.map(makeFolder) : [message("No subfolders")]));
    node.dataset.loaded = "true";
  };
  folderNavigation.querySelectorAll("details[data-folder]").forEach(bindFolder);
}

document.querySelectorAll("dialog[data-form-dialog]").forEach((dialog) => {
  const triggers = document.querySelectorAll(`[data-dialog-open="${dialog.id}"]`);
  dialog.querySelectorAll("[data-dialog-close]").forEach((button) => {
    button.addEventListener("click", () => dialog.close());
  });
  dialog.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && !event.defaultPrevented && !event.isComposing) {
      event.preventDefault();
      dialog.close();
    }
  });
  dialog.addEventListener("close", () => {
    dialog.querySelectorAll("input[name=confirm]").forEach((approval) => { approval.checked = false; });
    if (triggers.length) triggers[0].focus();
  });
  triggers.forEach((trigger) => {
    trigger.addEventListener("click", () => dialog.showModal());
    trigger.disabled = false;
  });
  if (dialog.dataset.autoOpen === "true" && triggers.length) dialog.showModal();
});

document.querySelectorAll("[data-name-form]").forEach((form) => {
  const name = form.querySelector("[name=display_name]");
  const location = form.querySelector("[name=location_id]");
  const output = form.querySelector("[data-generated-id]");
  const updateId = () => {
    const slug = name.value.normalize("NFKD").replace(/[^\x00-\x7F]/g, "")
      .toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
    output.textContent = !slug ? "Enter a name with letters or digits"
      : location && !location.value ? "Select a location"
      : location ? `${location.value}-${slug}` : slug;
  };
  name.addEventListener("input", updateId);
  if (location) location.addEventListener("change", updateId);
  updateId();
});

const cameraLocation = document.querySelector("#camera-location");
if (cameraLocation) {
  const camera = document.querySelector("#camera-id");
  const options = Array.from(document.querySelector("#camera-options").content.querySelectorAll("option"));
  const updateCameras = (selected = "") => {
    const matching = options.filter((option) => option.dataset.location === cameraLocation.value);
    const placeholder = !cameraLocation.value ? "Select a location first"
      : matching.length ? "Select a camera" : "No registered cameras at this location";
    camera.replaceChildren(new Option(placeholder, ""), ...matching.map((option) => option.cloneNode(true)));
    camera.disabled = matching.length === 0;
    camera.value = selected;
  };
  cameraLocation.addEventListener("change", () => updateCameras());
  updateCameras(camera.dataset.selected);
}

const scope = document.querySelector("#calibration-scope");
if (scope) {
  const updateScope = () => {
    document.querySelector("#camera-fields").disabled = scope.value === "video";
    document.querySelector("#camera-fields").hidden = scope.value === "video";
    document.querySelector("#video-fields").disabled = scope.value !== "video";
    document.querySelector("#video-fields").hidden = scope.value !== "video";
  };
  scope.addEventListener("change", updateScope);
  updateScope();
}

const lineFieldNames = ["counting_line_x1", "counting_line_y1", "counting_line_x2", "counting_line_y2"];
const canvas = document.querySelector("#line-canvas");
if (canvas) {
  const image = document.querySelector("#preview-image");
  const context = canvas.getContext("2d");
  const fields = lineFieldNames.map((name) => document.getElementById(name));
  const instructions = document.querySelector("#line-instructions");
  let endpoint = 0;
  const draw = () => {
    if (!image.complete || !image.naturalWidth) return;
    context.drawImage(image, 0, 0, canvas.width, canvas.height);
    const [x1, y1, x2, y2] = fields.map((field) => Number(field.value));
    if (![x1, y1, x2, y2].every(Number.isFinite)) return;
    context.lineWidth = Math.max(2, canvas.width / 250);
    context.strokeStyle = "#00ffcc";
    context.fillStyle = "#00ffcc";
    context.beginPath();
    context.moveTo(x1, y1);
    context.lineTo(x2, y2);
    context.stroke();
    const angle = Math.atan2(y2 - y1, x2 - x1);
    const length = Math.max(12, canvas.width / 45);
    context.beginPath();
    context.moveTo(x2, y2);
    context.lineTo(x2 - length * Math.cos(angle - .5), y2 - length * Math.sin(angle - .5));
    context.lineTo(x2 - length * Math.cos(angle + .5), y2 - length * Math.sin(angle + .5));
    context.closePath();
    context.fill();
  };
  image.addEventListener("load", () => {
    draw();
    instructions.textContent = "Frame ready. Your current line is preserved; click endpoints or edit coordinates.";
  });
  document.querySelector("#preview-form").addEventListener("submit", (event) => {
    event.preventDefault();
    const url = new URL(image.src);
    url.searchParams.set("seconds", event.currentTarget.querySelector("[name=seconds]").value);
    instructions.textContent = "Loading preview frame...";
    image.src = url.toString();
  });
  image.addEventListener("error", () => { instructions.textContent = "Frame could not be loaded. Choose an earlier preview time."; });
  fields.forEach((field) => field.addEventListener("input", draw));
  document.querySelector("#redraw-line").addEventListener("click", () => {
    endpoint = 0;
    instructions.textContent = "Click the first endpoint, then the second.";
    canvas.focus();
  });
  canvas.addEventListener("pointerdown", (event) => {
    const bounds = canvas.getBoundingClientRect();
    fields[endpoint * 2].value = Math.max(0, Math.min(canvas.width - 1,
      Math.round((event.clientX - bounds.left) * canvas.width / bounds.width)));
    fields[endpoint * 2 + 1].value = Math.max(0, Math.min(canvas.height - 1,
      Math.round((event.clientY - bounds.top) * canvas.height / bounds.height)));
    endpoint = 1 - endpoint;
    instructions.textContent = endpoint ? "Now click the second endpoint." : "Line selected. Review and save the calibration.";
    draw();
    canvas.dispatchEvent(new Event("counting-line-change", { bubbles: true }));
  });
  draw();
}

const readinessPanel = document.querySelector("#review-readiness");
if (readinessPanel) {
  const camera = document.querySelector("#review-camera");
  const captureTime = document.querySelector("#capture-override");
  const geometry = document.querySelector("#geometry-action");
  const coordinates = lineFieldNames.map((name) => document.getElementById(name)).filter(Boolean);
  const watched = [camera, captureTime, geometry, ...coordinates].filter(Boolean);
  const initialCaptureTime = captureTime.value;
  const status = document.querySelector("#readiness-update-status");
  const results = document.querySelector("#readiness-results");
  const retry = document.querySelector("#retry-readiness");
  let lineEdited = false;
  let generation = 0;
  let running = false;
  let timer;
  let layoutFrozen = false;
  const freezeLayout = () => {
    if (layoutFrozen) return;
    // Keep readiness updates from moving the canvas during endpoint placement.
    results.style.height = `${results.getBoundingClientRect().height}px`;
    results.style.overflowY = "auto";
    status.style.minHeight = `${status.getBoundingClientRect().height}px`;
    layoutFrozen = true;
  };
  const draftBody = () => {
    const body = new FormData();
    body.set("csrf", document.querySelector("input[name=csrf]").value);
    body.set("video", readinessPanel.dataset.video);
    body.set("camera_id", camera.value);
    if (geometry) body.set("geometry_action", geometry.value);
    if (captureTime.value !== initialCaptureTime) body.set("captured_at_utc", captureTime.value);
    if (lineEdited) {
      body.set("line_override", "yes");
      coordinates.forEach((field) => body.set(field.id, field.value));
    }
    return body;
  };
  const checkReadiness = async () => {
    if (running) return;
    const sent = generation;
    running = true;
    try {
      const response = await fetch(readinessPanel.dataset.readinessUrl, { method: "POST", body: draftBody() });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const markup = await response.text();
      if (sent === generation) {
        results.innerHTML = markup;
        results.style.visibility = "visible";
        results.setAttribute("aria-hidden", "false");
        readinessPanel.setAttribute("aria-busy", "false");
        status.textContent = "Readiness updated for unsaved values. Save each section to apply changes.";
      }
    } catch (error) {
      if (sent === generation) {
        status.setAttribute("role", "alert");
        status.textContent = `Readiness could not be checked: ${error.message}. Retry without losing unsaved changes.`;
        retry.hidden = false;
        readinessPanel.setAttribute("aria-busy", "false");
      }
    } finally {
      running = false;
      if (sent !== generation) {
        clearTimeout(timer);
        timer = setTimeout(checkReadiness, 350);
      }
    }
  };
  const scheduleCheck = () => {
    generation += 1;
    clearTimeout(timer);
    freezeLayout();
    results.style.visibility = "hidden";
    results.setAttribute("aria-hidden", "true");
    readinessPanel.setAttribute("aria-busy", "true");
    status.setAttribute("role", "status");
    status.textContent = "Checking unsaved values...";
    retry.hidden = true;
    timer = setTimeout(checkReadiness, 350);
  };
  const changed = (event) => {
    const drawn = event.type === "counting-line-change";
    if (!drawn && !watched.includes(event.target)) return;
    if (drawn || coordinates.includes(event.target)) lineEdited = true;
    scheduleCheck();
  };
  document.addEventListener("input", changed);
  document.addEventListener("change", changed);
  document.addEventListener("counting-line-change", changed);
  retry.addEventListener("click", scheduleCheck);
}

const cloudAction = document.querySelector("#cloud-action");
if (cloudAction) {
  const wait = document.querySelector("#wait-processing");
  const updateWait = () => {
    wait.disabled = cloudAction.value !== "submit";
    document.querySelector("#wait-options").hidden = wait.disabled;
    if (wait.disabled) wait.checked = false;
  };
  cloudAction.addEventListener("change", updateWait);
  updateWait();
}
