"use strict";

const loadPortal = () => require("../../src/people_counter/portal_static/portal.js");
const change = (element, type = "change") => element.dispatchEvent(new Event(type, { bubbles: true }));
const settle = () => new Promise((resolve) => setTimeout(resolve, 0));

beforeEach(() => {
  jest.resetModules();
  document.body.innerHTML = "";
  global.fetch = jest.fn();
  jest.spyOn(document, "addEventListener");
});

afterEach(() => {
  for (const args of document.addEventListener.mock.calls) document.removeEventListener(...args);
  jest.restoreAllMocks();
  delete global.fetch;
});

test("loads on pages without optional portal controls", () => {
  loadPortal();
  expect(fetch).not.toHaveBeenCalled();
});

test("opens all enclosing disclosures for invalid controls", () => {
  document.body.innerHTML = "<details><details><input required></details></details><input id=outside>";
  loadPortal();
  document.querySelector("details input").dispatchEvent(new Event("invalid"));
  expect([...document.querySelectorAll("details")].every((node) => node.open)).toBe(true);
  change(document.querySelector("#outside"), "invalid");
});

function folderPage() {
  document.body.innerHTML = `
    <button id="toggle-folders" aria-expanded="false"></button>
    <aside id="folder-sidebar" class="hidden">
      <nav id="folder-navigation" data-folders-url="/folders" data-browse-url="/"
        data-current-folder="site/east" data-query="clip" data-kind="videos">
        <a class="folder-link"><svg><path d="M0 0h8v8H0z"></path></svg></a>
        <details data-folder="site"><summary tabindex="0">site</summary><ul data-folder-children></ul></details>
      </nav>
    </aside>`;
  loadPortal();
  return document.querySelector("details");
}

async function expand(node) {
  node.open = true;
  await settle();
}

test("toggles the responsive folder sidebar", () => {
  folderPage();
  const toggle = document.querySelector("#toggle-folders");
  toggle.click();
  expect(toggle.getAttribute("aria-expanded")).toBe("true");
  expect(document.querySelector("#folder-sidebar").classList.contains("hidden")).toBe(false);
  toggle.click();
  expect(toggle.getAttribute("aria-expanded")).toBe("false");
  expect(document.querySelector("#folder-sidebar").classList.contains("hidden")).toBe(true);
});

test("loads folders lazily, retains filters, escapes names and caches children", async () => {
  fetch.mockResolvedValue({ ok: true, json: async () => ({ folders: [
    { name: "east", path: "site/east" },
    { name: "<img src=x onerror=alert(1)>", path: "site/unsafe" },
  ] }) });
  const node = folderPage();
  expect(fetch).not.toHaveBeenCalled();
  change(node, "toggle");
  expect(fetch).not.toHaveBeenCalled();
  await expand(node);
  expect(fetch).toHaveBeenCalledTimes(1);
  expect(new URL(fetch.mock.calls[0][0]).searchParams.get("folder")).toBe("site");
  const link = node.querySelector(".folder-link");
  const url = new URL(link.href);
  expect([...url.searchParams]).toEqual([["folder", "site/east"], ["q", "clip"], ["kind", "videos"]]);
  expect(link.getAttribute("aria-current")).toBe("page");
  expect(link.closest("details").className).toBe("folder-node");
  expect(link.querySelector("svg path").getAttribute("d")).toBe("M0 0h8v8H0z");
  const unsafe = node.querySelectorAll(".folder-link")[1];
  expect(unsafe.textContent).toBe("<img src=x onerror=alert(1)>");
  expect(unsafe.hasAttribute("aria-current")).toBe(false);
  expect(node.querySelector("img")).toBeNull();
  expect(node.dataset.loaded).toBe("true");
  expect(node.dataset.loading).toBe("false");
  node.open = false;
  change(node, "toggle");
  await expand(node);
  expect(fetch).toHaveBeenCalledTimes(1);
  fetch.mockResolvedValue({ ok: true, json: async () => ({ folders: [] }) });
  await expand(node.querySelector("details"));
  expect(node.querySelector("details").textContent).toContain("No subfolders");
  expect(fetch).toHaveBeenCalledTimes(2);
});

test("shows loading state and does not duplicate an in-flight folder request", async () => {
  let finish;
  fetch.mockReturnValue(new Promise((resolve) => { finish = resolve; }));
  const node = folderPage();
  node.open = true;
  change(node, "toggle");
  change(node, "toggle");
  expect(node.querySelector('[role="status"]').textContent).toBe("Loading folders...");
  expect(node.querySelector('[role="status"]').className).toBe("folder-message");
  expect(fetch).toHaveBeenCalledTimes(1);
  finish({ ok: true, json: async () => ({ folders: [] }) });
  await settle();
  expect(node.textContent).toContain("No subfolders");
  expect(node.querySelector('[role="status"]')).toBeNull();
});

test.each([
  ["HTTP 503", () => Promise.resolve({ ok: false, status: 503 })],
  ["Unexpected folder response", () => Promise.resolve({ ok: true, json: async () => ({}) })],
  ["Unexpected folder response", () => Promise.resolve({
    ok: true, json: async () => ({ folders: [{ name: 7, path: "bad" }] }),
  })],
  ["Unexpected folder response", () => Promise.resolve({
    ok: true, json: async () => ({ folders: [{ name: "bad", path: 7 }] }),
  })],
  ["offline", () => Promise.reject(new Error("offline"))],
  ["invalid JSON", () => Promise.resolve({ ok: true, json: async () => { throw new Error("invalid JSON"); } })],
])("reports folder failure %s and allows a focused retry", async (message, response) => {
  fetch.mockImplementationOnce(response);
  fetch.mockResolvedValue({ ok: true, json: async () => ({ folders: [] }) });
  const node = folderPage();
  await expand(node);
  expect(node.querySelector('[role="alert"]').textContent).toContain(`Unable to load folders: ${message}`);
  expect(node.dataset.loading).toBe("false");
  expect(node.dataset.loaded).not.toBe("true");
  const retry = node.querySelector("button");
  expect(retry.getAttribute("aria-label")).toBe("Retry folder site");
  expect(retry.type).toBe("button");
  expect(retry.className).toBe("btn-secondary btn-sm");
  expect(retry.textContent).toBe("Retry");
  retry.click();
  await settle();
  expect(node.querySelector('[role="alert"]')).toBeNull();
  expect(node.textContent).toContain("No subfolders");
  expect(document.activeElement).toBe(node.querySelector("summary"));
  expect(fetch).toHaveBeenCalledTimes(2);
});

function dialogPage(autoOpen = false, withTrigger = true) {
  document.body.innerHTML = `
    ${withTrigger ? '<button data-dialog-open="edit" disabled>Open</button>' : ""}
    <dialog id="edit" data-form-dialog data-auto-open="${autoOpen}">
      <input type="checkbox" name="confirm" checked><button data-dialog-close>Close</button>
    </dialog>`;
  const dialog = document.querySelector("dialog");
  dialog.showModal = jest.fn(() => { dialog.open = true; });
  dialog.close = jest.fn(() => { dialog.open = false; change(dialog, "close"); });
  loadPortal();
  return dialog;
}

test("enables dialog triggers and clears approval and restores focus on close", () => {
  const dialog = dialogPage();
  const trigger = document.querySelector("[data-dialog-open]");
  expect(trigger.disabled).toBe(false);
  expect(dialog.showModal).not.toHaveBeenCalled();
  trigger.click();
  expect(dialog.open).toBe(true);
  dialog.querySelector("button").click();
  expect(dialog.open).toBe(false);
  expect(dialog.querySelector("input").checked).toBe(false);
  expect(document.activeElement).toBe(trigger);
});

test.each([
  [{ key: "Escape" }, true],
  [{ key: "Enter" }, false],
  [{ key: "Escape", isComposing: true }, false],
  [{ key: "Escape", prevented: true }, false],
])("handles dialog keydown %j without overriding other handlers", (options, closes) => {
  const dialog = dialogPage(true);
  expect(dialog.showModal).toHaveBeenCalledTimes(1);
  const event = new KeyboardEvent("keydown", { ...options, cancelable: true });
  if (options.prevented) event.preventDefault();
  dialog.dispatchEvent(event);
  expect(dialog.close).toHaveBeenCalledTimes(closes ? 1 : 0);
  expect(event.defaultPrevented).toBe(closes || Boolean(options.prevented));
});

test("does not auto-open a dialog without a trigger and can close it safely", () => {
  const dialog = dialogPage(true, false);
  expect(dialog.showModal).not.toHaveBeenCalled();
  dialog.close();
  expect(dialog.querySelector("input").checked).toBe(false);
});

test.each([false, true])("generates stable name IDs with location control present: %s", (withLocation) => {
  document.body.innerHTML = `
    <form data-name-form><input name="display_name">
      ${withLocation ? '<select name="location_id"><option value="">Choose</option><option value="north">North</option></select>' : ""}
      <output data-generated-id></output>
    </form>`;
  loadPortal();
  const name = document.querySelector("input");
  const output = document.querySelector("output");
  expect(output.textContent).toBe("Enter a name with letters or digits");
  name.value = " --Caf\u00e9 / ENTRANCE 17-- ";
  change(name, "input");
  expect(output.textContent).toBe(withLocation ? "Select a location" : "cafe-entrance-17");
  if (withLocation) {
    const location = document.querySelector("select");
    location.value = "north";
    change(location);
    expect(output.textContent).toBe("north-cafe-entrance-17");
  }
  name.value = "---";
  change(name, "input");
  expect(output.textContent).toBe("Enter a name with letters or digits");
});

test("filters cameras by location, preserves the initial selection and resets changes", () => {
  document.body.innerHTML = `
    <select id="camera-location"><option value="">Choose</option><option selected>north</option><option>south</option><option>empty</option></select>
    <select id="camera-id" data-selected="north-2"></select>
    <template id="camera-options"><option value="north-1" data-location="north">One</option>
      <option value="north-2" data-location="north">Two</option><option value="south-1" data-location="south">South</option></template>`;
  loadPortal();
  const camera = document.querySelector("#camera-id");
  const location = document.querySelector("#camera-location");
  expect(camera.value).toBe("north-2");
  expect(camera.disabled).toBe(false);
  expect([...camera.options].map((option) => option.value)).toEqual(["", "north-1", "north-2"]);
  expect([...camera.options].map((option) => option.textContent)).toEqual(["Select a camera", "One", "Two"]);
  location.value = "south";
  change(location);
  expect(camera.value).toBe("");
  expect([...camera.options].map((option) => option.value)).toEqual(["", "south-1"]);
  location.value = "empty";
  change(location);
  expect(camera.disabled).toBe(true);
  expect(camera.options[0].textContent).toBe("No registered cameras at this location");
  location.value = "";
  change(location);
  expect(camera.disabled).toBe(true);
  expect(camera.options[0].textContent).toBe("Select a location first");
});

test("enables only the selected calibration scope", () => {
  document.body.innerHTML = `
    <select id="calibration-scope"><option>camera</option><option>video</option></select>
    <fieldset id="camera-fields"></fieldset><fieldset id="video-fields"></fieldset>`;
  loadPortal();
  const camera = document.querySelector("#camera-fields");
  const video = document.querySelector("#video-fields");
  expect([camera.disabled, camera.hidden, video.disabled, video.hidden]).toEqual([false, false, true, true]);
  document.querySelector("select").value = "video";
  change(document.querySelector("select"));
  expect([camera.disabled, camera.hidden, video.disabled, video.hidden]).toEqual([true, true, false, false]);
});

function canvasPage({ complete = true, naturalWidth = 320 } = {}) {
  document.body.innerHTML = `
    <img id="preview-image" src="http://localhost/preview?video=clip.mp4">
    <canvas id="line-canvas" width="320" height="180" tabindex="0"></canvas>
    <input id="counting_line_x1" value="10"><input id="counting_line_y1" value="20">
    <input id="counting_line_x2" value="100"><input id="counting_line_y2" value="120">
    <p id="line-instructions"></p><form id="preview-form"><input name="seconds" value="2.5"></form>
    <button id="redraw-line"></button>`;
  const canvas = document.querySelector("canvas");
  const image = document.querySelector("img");
  Object.defineProperties(image, {
    complete: { configurable: true, value: complete },
    naturalWidth: { configurable: true, value: naturalWidth },
  });
  const context = Object.fromEntries([
    "drawImage", "beginPath", "moveTo", "lineTo", "stroke", "closePath", "fill",
  ].map((method) => [method, jest.fn()]));
  jest.spyOn(HTMLCanvasElement.prototype, "getContext").mockReturnValue(context);
  jest.spyOn(canvas, "getBoundingClientRect").mockReturnValue({ left: 10, top: 20, width: 640, height: 360 });
  loadPortal();
  return { canvas, image, context, instructions: document.querySelector("#line-instructions") };
}

test("draws the saved line and direction arrow when the frame is available", () => {
  const { image, context } = canvasPage();
  expect(context.drawImage).toHaveBeenCalledWith(image, 0, 0, 320, 180);
  expect(HTMLCanvasElement.prototype.getContext).toHaveBeenCalledWith("2d");
  expect(context.moveTo).toHaveBeenNthCalledWith(1, 10, 20);
  expect(context.moveTo).toHaveBeenNthCalledWith(2, 100, 120);
  expect(context.lineTo).toHaveBeenNthCalledWith(1, 100, 120);
  expect(context.strokeStyle).toBe("#00ffcc");
  expect(context.fillStyle).toBe("#00ffcc");
  expect(context.lineWidth).toBe(2);
  expect(context.beginPath).toHaveBeenCalledTimes(2);
  expect(context.stroke).toHaveBeenCalledTimes(1);
  expect(context.lineTo).toHaveBeenCalledTimes(3);
  expect(context.lineTo).toHaveBeenNthCalledWith(2,
    expect.closeTo(88.6788882472, 8), expect.closeTo(116.0210015480, 8));
  expect(context.lineTo).toHaveBeenNthCalledWith(3,
    expect.closeTo(97.2313889610, 8), expect.closeTo(108.3237509056, 8));
  expect(context.closePath).toHaveBeenCalledTimes(1);
  expect(context.fill).toHaveBeenCalledTimes(1);
});

test.each([{ complete: false }, { naturalWidth: 0 }])("waits for a usable frame: %j", (options) => {
  const { image, context, instructions } = canvasPage(options);
  expect(context.drawImage).not.toHaveBeenCalled();
  Object.defineProperties(image, { complete: { value: true }, naturalWidth: { value: 320 } });
  change(image, "load");
  expect(context.stroke).toHaveBeenCalledTimes(1);
  expect(instructions.textContent).toContain("Frame ready. Your current line is preserved");
});

test("redraws edited coordinates but does not draw non-finite input", () => {
  const { context } = canvasPage();
  const x = document.querySelector("#counting_line_x1");
  x.value = "40";
  change(x, "input");
  expect(context.moveTo).toHaveBeenCalledWith(40, 20);
  context.stroke.mockClear();
  x.value = "not-a-number";
  change(x, "input");
  expect(context.stroke).not.toHaveBeenCalled();
});

test("requests another preview time without losing the source and reports image errors", () => {
  const { image, instructions } = canvasPage();
  const form = document.querySelector("form");
  const event = new Event("submit", { cancelable: true });
  form.dispatchEvent(event);
  expect(event.defaultPrevented).toBe(true);
  expect([...new URL(image.src).searchParams]).toEqual([["video", "clip.mp4"], ["seconds", "2.5"]]);
  expect(instructions.textContent).toBe("Loading preview frame...");
  expect(document.querySelector("#counting_line_x1").value).toBe("10");
  change(image, "error");
  expect(instructions.textContent).toContain("Frame could not be loaded");
});

test("maps pointer positions to clamped image coordinates and resets the endpoint", () => {
  const { canvas, context, instructions } = canvasPage();
  const point = (x, y) => canvas.dispatchEvent(new MouseEvent("pointerdown", { clientX: x, clientY: y }));
  context.stroke.mockClear();
  point(74, 200);
  expect(context.stroke).toHaveBeenCalledTimes(1);
  expect(document.querySelector("#counting_line_x1").value).toBe("32");
  expect(document.querySelector("#counting_line_y1").value).toBe("90");
  expect(instructions.textContent).toBe("Now click the second endpoint.");
  point(1000, -50);
  expect(document.querySelector("#counting_line_x2").value).toBe("319");
  expect(document.querySelector("#counting_line_y2").value).toBe("0");
  expect(instructions.textContent).toBe("Line selected. Review and save the calibration.");
  point(100, 100);
  document.querySelector("#redraw-line").click();
  expect(document.activeElement).toBe(canvas);
  expect(instructions.textContent).toBe("Click the first endpoint, then the second.");
  point(-100, 1000);
  expect(document.querySelector("#counting_line_x1").value).toBe("0");
  expect(document.querySelector("#counting_line_y1").value).toBe("179");
});

test("allows processing waits only for explicit backfill submission and clears stale approval", () => {
  document.body.innerHTML = `
    <select id="cloud-action"><option value="">Choose</option><option>publish</option><option>submit</option></select>
    <input id="wait-processing" type="checkbox" checked><div id="wait-options"></div>`;
  loadPortal();
  const action = document.querySelector("select");
  const wait = document.querySelector("input");
  const options = document.querySelector("#wait-options");
  expect([wait.disabled, wait.checked, options.hidden]).toEqual([true, false, true]);
  action.value = "submit";
  change(action);
  expect([wait.disabled, options.hidden]).toEqual([false, false]);
  wait.checked = true;
  change(action);
  expect(wait.checked).toBe(true);
  action.value = "publish";
  change(action);
  expect([wait.disabled, wait.checked, options.hidden]).toEqual([true, false, true]);
});
