"use strict";

const scope = document.querySelector("#calibration-scope");
if (scope) {
  const updateScope = () => {
    document.querySelector("#camera-fields").disabled = scope.value === "video";
    document.querySelector("#video-fields").disabled = scope.value !== "video";
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
    if (wait.disabled) wait.checked = false;
  };
  cloudAction.addEventListener("change", updateWait);
  updateWait();
}
