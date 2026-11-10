"use strict";

document.addEventListener("invalid", (event) => {
  for (let section = event.target.closest("details"); section; section = section.parentElement.closest("details")) {
    section.open = true;
  }
}, true);

const profileDialog = document.querySelector("#profile-dialog");
if (profileDialog) {
  const openProfileDialog = document.querySelector("#open-profile-dialog");
  openProfileDialog.addEventListener("click", () => profileDialog.showModal());
  profileDialog.querySelectorAll("[data-close-profile-dialog]").forEach((button) => {
    button.addEventListener("click", () => profileDialog.close());
  });
  profileDialog.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && !event.defaultPrevented && !event.isComposing) {
      event.preventDefault();
      profileDialog.close();
    }
  });
  openProfileDialog.disabled = false;
}

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

const canvas = document.querySelector("#line-canvas");
if (canvas) {
  const image = document.querySelector("#preview-image");
  const context = canvas.getContext("2d");
  const fields = ["counting_line_x1", "counting_line_y1", "counting_line_x2", "counting_line_y2"]
    .map((name) => document.getElementById(name));
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
  });
  draw();
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
