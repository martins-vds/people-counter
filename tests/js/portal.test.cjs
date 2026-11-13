"use strict";

require("@testing-library/jest-dom");
const { fireEvent, screen, waitFor, within } = require("@testing-library/dom");
const userEvent = require("@testing-library/user-event").default;

const loadPortal = () => require("../../src/people_counter/portal_static/portal.js");

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
  document.body.innerHTML = '<details><summary>Settings</summary><details><summary>Coordinates</summary><input aria-label="X1" required></details></details><input aria-label="Outside">';
  loadPortal();
  fireEvent.invalid(screen.getByLabelText("X1"));
  expect(screen.getByLabelText("X1")).toBeVisible();
  for (const section of document.querySelectorAll("details")) expect(section).toHaveAttribute("open");
  fireEvent.invalid(screen.getByLabelText("Outside"));
});

function folderPage() {
  const user = userEvent.setup();
  document.body.innerHTML = `
    <style>.hidden { display: none; }</style>
    <button id="toggle-folders" aria-expanded="false">Folders</button>
    <aside id="folder-sidebar" class="hidden">
      <nav aria-label="Folders" id="folder-navigation" data-folders-url="/folders" data-browse-url="/"
        data-current-folder="site/east" data-query="clip" data-kind="videos">
        <a class="folder-link" href="/"><svg aria-hidden="true"><path d="M0 0h8v8H0z"></path></svg>Source root</a>
        <details data-folder="site"><summary tabindex="0">site</summary><ul data-folder-children></ul></details>
      </nav>
    </aside>`;
  loadPortal();
  return { user, node: document.querySelector("details") };
}

test("makes folder navigation available only while its disclosure is expanded", async () => {
  const { user } = folderPage();
  const toggle = screen.getByRole("button", { name: "Folders" });
  const navigation = screen.getByLabelText("Folders", { selector: "nav" });
  expect(toggle).toHaveAttribute("aria-expanded", "false");
  expect(navigation).not.toBeVisible();
  await user.click(toggle);
  expect(toggle).toHaveAttribute("aria-expanded", "true");
  expect(screen.getByRole("navigation", { name: "Folders" })).toBeVisible();
  await user.click(toggle);
  expect(toggle).toHaveAttribute("aria-expanded", "false");
  expect(navigation).not.toBeVisible();
});

test("loads folders lazily, retains filters, escapes names and caches children", async () => {
  fetch.mockResolvedValue({ ok: true, json: async () => ({ folders: [
    { name: "east", path: "site/east" },
    { name: "<img src=x onerror=alert(1)>", path: "site/unsafe" },
  ] }) });
  const { user, node } = folderPage();
  await user.click(screen.getByRole("button", { name: "Folders" }));
  expect(fetch).not.toHaveBeenCalled();
  fireEvent(node, new Event("toggle"));
  expect(fetch).not.toHaveBeenCalled();
  const summary = node.querySelector("summary");
  await user.click(summary);
  const link = await within(node).findByRole("link", { name: "east", current: "page" });
  expect(fetch).toHaveBeenCalledTimes(1);
  expect(new URL(fetch.mock.calls[0][0]).searchParams.get("folder")).toBe("site");
  const url = new URL(link.href);
  expect(url.pathname).toBe("/");
  expect(url.searchParams.get("folder")).toBe("site/east");
  expect(url.searchParams.get("q")).toBe("clip");
  expect(url.searchParams.get("kind")).toBe("videos");
  expect(link).toBeVisible();
  const unsafe = within(node).getByRole("link", { name: "<img src=x onerror=alert(1)>" });
  expect(unsafe).not.toHaveAttribute("aria-current");
  expect(node.querySelector("img")).not.toBeInTheDocument();
  await user.click(summary);
  expect(link).not.toBeVisible();
  await user.click(summary);
  expect(link).toBeVisible();
  expect(fetch).toHaveBeenCalledTimes(1);
  fetch.mockResolvedValue({ ok: true, json: async () => ({ folders: [] }) });
  await user.click(link.closest("summary"));
  expect(await within(node).findByText("No subfolders")).toBeVisible();
  expect(fetch).toHaveBeenCalledTimes(2);
});

test("shows loading state and does not duplicate an in-flight folder request", async () => {
  let finish;
  fetch.mockReturnValue(new Promise((resolve) => { finish = resolve; }));
  const { user, node } = folderPage();
  await user.click(screen.getByRole("button", { name: "Folders" }));
  await user.click(node.querySelector("summary"));
  expect(await screen.findByRole("status")).toHaveTextContent("Loading folders...");
  fireEvent(node, new Event("toggle"));
  expect(fetch).toHaveBeenCalledTimes(1);
  finish({ ok: true, json: async () => ({ folders: [] }) });
  expect(await screen.findByText("No subfolders")).toBeVisible();
  expect(screen.queryByRole("status")).not.toBeInTheDocument();
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
  const { user, node } = folderPage();
  await user.click(screen.getByRole("button", { name: "Folders" }));
  await user.click(node.querySelector("summary"));
  expect(await screen.findByRole("alert")).toHaveTextContent(`Unable to load folders: ${message}`);
  const retry = screen.getByRole("button", { name: "Retry folder site" });
  expect(retry).toHaveAttribute("type", "button");
  expect(retry).toHaveTextContent("Retry");
  await user.click(retry);
  expect(await screen.findByText("No subfolders")).toBeVisible();
  expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  await waitFor(() => expect(node.querySelector("summary")).toHaveFocus());
  expect(fetch).toHaveBeenCalledTimes(2);
});

function dialogPage(autoOpen = false, withTrigger = true) {
  const user = userEvent.setup();
  document.body.innerHTML = `
    ${withTrigger ? '<button data-dialog-open="edit" disabled>Open</button>' : ""}
    <dialog id="edit" aria-labelledby="edit-title" data-form-dialog data-auto-open="${autoOpen}">
      <h2 id="edit-title">Edit settings</h2>
      <label><input type="checkbox" name="confirm" checked>I approve this action</label>
      <button data-dialog-close>Close dialog</button>
    </dialog>`;
  const dialog = document.querySelector("dialog");
  dialog.showModal = jest.fn(() => { dialog.open = true; });
  dialog.close = jest.fn(() => { dialog.open = false; fireEvent(dialog, new Event("close")); });
  loadPortal();
  return { user, dialog };
}

test("enables dialog triggers and clears approval and restores focus on close", async () => {
  const { user, dialog } = dialogPage();
  const trigger = screen.getByRole("button", { name: "Open" });
  const approval = screen.getByLabelText("I approve this action");
  expect(trigger).toBeEnabled();
  expect(dialog).not.toHaveAttribute("open");
  await user.click(trigger);
  expect(screen.getByRole("dialog", { name: "Edit settings" })).toHaveAttribute("open");
  await user.click(screen.getByRole("button", { name: "Close dialog" }));
  expect(dialog).not.toHaveAttribute("open");
  expect(approval).not.toBeChecked();
  expect(trigger).toHaveFocus();
  await user.click(trigger);
  expect(screen.getByRole("dialog", { name: "Edit settings" })).toHaveAttribute("open");
  expect(approval).not.toBeChecked();
});

test.each([
  [{ key: "Escape" }, true],
  [{ key: "Enter" }, false],
  [{ key: "Escape", isComposing: true }, false],
  [{ key: "Escape", prevented: true }, false],
])("handles dialog keydown %j without overriding other handlers", (options, closes) => {
  const { dialog } = dialogPage(true);
  expect(screen.getByRole("dialog", { name: "Edit settings" })).toHaveAttribute("open");
  const event = new KeyboardEvent("keydown", { ...options, cancelable: true });
  if (options.prevented) event.preventDefault();
  dialog.dispatchEvent(event);
  if (closes) expect(dialog).not.toHaveAttribute("open");
  else expect(dialog).toHaveAttribute("open");
  expect(event.defaultPrevented).toBe(closes || Boolean(options.prevented));
});

test("does not auto-open a dialog without a trigger and can close it safely", () => {
  const { dialog } = dialogPage(true, false);
  expect(dialog).not.toHaveAttribute("open");
  dialog.close();
  expect(screen.getByLabelText("I approve this action")).not.toBeChecked();
});

test.each([false, true])("generates stable name IDs with location control present: %s", async (withLocation) => {
  const user = userEvent.setup();
  const nameLabel = withLocation ? "Camera name" : "Location name";
  document.body.innerHTML = `
    <form data-name-form><label for="display-name">${nameLabel}</label><input id="display-name" name="display_name">
      ${withLocation ? '<label for="location">Location</label><select id="location" name="location_id"><option value="">Choose</option><option value="north">North</option></select>' : ""}
      <output data-generated-id aria-live="polite"></output>
    </form>`;
  loadPortal();
  const name = screen.getByRole("textbox", { name: nameLabel });
  const output = screen.getByRole("status");
  expect(output).toHaveTextContent(/^Enter a name with letters or digits$/);
  await user.type(name, " --Caf\u00e9 / ENTRANCE 17-- ");
  expect(output).toHaveTextContent(withLocation ? /^Select a location$/ : /^cafe-entrance-17$/);
  if (withLocation) {
    await user.selectOptions(screen.getByRole("combobox", { name: "Location" }), "north");
    expect(output).toHaveTextContent(/^north-cafe-entrance-17$/);
  }
  await user.clear(name);
  await user.type(name, "---");
  expect(output).toHaveTextContent(/^Enter a name with letters or digits$/);
});

test("filters cameras by location, preserves the initial selection and resets changes", async () => {
  const user = userEvent.setup();
  document.body.innerHTML = `
    <label for="camera-location">Location</label>
    <select id="camera-location"><option value="">Choose</option><option selected>north</option><option>south</option><option>empty</option></select>
    <label for="camera-id">Camera</label>
    <select id="camera-id" data-selected="north-2"></select>
    <template id="camera-options"><option value="north-1" data-location="north">One</option>
      <option value="north-2" data-location="north">Two</option><option value="south-1" data-location="south">South</option></template>`;
  loadPortal();
  const camera = screen.getByRole("combobox", { name: "Camera" });
  const location = screen.getByRole("combobox", { name: "Location" });
  expect(camera).toHaveValue("north-2");
  expect(camera).toHaveDisplayValue("Two");
  expect(camera).toBeEnabled();
  expect(within(camera).getAllByRole("option")).toHaveLength(3);
  expect(within(camera).getByRole("option", { name: "Select a camera" })).toHaveValue("");
  expect(within(camera).getByRole("option", { name: "One" })).toHaveValue("north-1");
  expect(within(camera).getByRole("option", { name: "Two" })).toHaveValue("north-2");
  await user.selectOptions(location, "south");
  expect(camera).toHaveValue("");
  expect(camera).toHaveDisplayValue("Select a camera");
  expect(within(camera).getAllByRole("option")).toHaveLength(2);
  expect(within(camera).getByRole("option", { name: "South" })).toHaveValue("south-1");
  expect(within(camera).queryByRole("option", { name: "One" })).not.toBeInTheDocument();
  await user.selectOptions(location, "empty");
  expect(camera).toBeDisabled();
  expect(camera).toHaveDisplayValue("No registered cameras at this location");
  await user.selectOptions(location, "");
  expect(camera).toBeDisabled();
  expect(camera).toHaveDisplayValue("Select a location first");
});

test("makes only the selected calibration scope visible and submittable", async () => {
  const user = userEvent.setup();
  document.body.innerHTML = `
    <form>
    <label for="calibration-scope">Apply calibration to</label>
    <select id="calibration-scope" name="scope"><option>camera</option><option>video</option></select>
    <fieldset id="camera-fields"><legend>Camera metadata</legend>
      <label>Camera<input name="camera_id" value="north-2"></label>
    </fieldset>
    <fieldset id="video-fields"><legend>Video exception</legend>
      <label>Capture time override<input name="captured_at_utc" value="2026-01-01T12:00:00Z"></label>
    </fieldset>
    </form>`;
  loadPortal();
  const camera = screen.getByText("Camera metadata").closest("fieldset");
  const video = screen.getByText("Video exception").closest("fieldset");
  expect(camera).toBeEnabled();
  expect(camera).toBeVisible();
  expect(video).toBeDisabled();
  expect(video).not.toBeVisible();
  const scope = screen.getByRole("combobox", { name: "Apply calibration to" });
  expect(Object.fromEntries(new FormData(scope.form))).toEqual({ scope: "camera", camera_id: "north-2" });
  await user.selectOptions(scope, "video");
  expect(camera).toBeDisabled();
  expect(camera).not.toBeVisible();
  expect(video).toBeEnabled();
  expect(video).toBeVisible();
  expect(Object.fromEntries(new FormData(scope.form))).toEqual({
    scope: "video", captured_at_utc: "2026-01-01T12:00:00Z",
  });
  await user.selectOptions(scope, "camera");
  expect(camera).toBeEnabled();
  expect(camera).toBeVisible();
  expect(video).toBeDisabled();
  expect(video).not.toBeVisible();
  expect(Object.fromEntries(new FormData(scope.form))).toEqual({ scope: "camera", camera_id: "north-2" });
});

function canvasPage({ complete = true, naturalWidth = 320, width = 320, extra = "" } = {}) {
  const user = userEvent.setup();
  document.body.innerHTML = `
    <img id="preview-image" src="http://localhost/preview?video=clip.mp4" alt="Source video frame">
    <canvas id="line-canvas" width="${width}" height="180" tabindex="0" aria-label="Counting line editor"></canvas>
    <label for="counting_line_x1">X1</label><input id="counting_line_x1" value="10">
    <label for="counting_line_y1">Y1</label><input id="counting_line_y1" value="20">
    <label for="counting_line_x2">X2</label><input id="counting_line_x2" value="100">
    <label for="counting_line_y2">Y2</label><input id="counting_line_y2" value="120">
    <p id="line-instructions" role="status"></p><form id="preview-form">
      <label for="seconds">Preview time in seconds</label><input id="seconds" name="seconds" value="2.5">
      <button>Show frame</button>
    </form>
    <button id="redraw-line">Redraw line</button>${extra}`;
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
  return { user, canvas, image, context, instructions: document.querySelector("#line-instructions") };
}

test("renders the saved directed line and arrow through the canvas API", () => {
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

test.each([
  [10, 20, 100, 20],
  [100, 20, 10, 20],
  [10, 20, 10, 120],
  [10, 120, 10, 20],
])("keeps arrow wings symmetric and behind the directed endpoint (%s, %s -> %s, %s)", (x1, y1, x2, y2) => {
  const { context } = canvasPage();
  for (const [label, value] of Object.entries({ X1: x1, Y1: y1, X2: x2, Y2: y2 })) {
    screen.getByLabelText(label).value = String(value);
  }
  fireEvent.input(screen.getByLabelText("Y2"));
  const wings = context.lineTo.mock.calls.slice(-2);
  const crossProducts = [];
  for (const [x, y] of wings) {
    expect(Math.hypot(x - x2, y - y2)).toBeCloseTo(12, 8);
    expect((x - x2) * (x1 - x2) + (y - y2) * (y1 - y2)).toBeGreaterThan(0);
    crossProducts.push((x - x2) * (y1 - y2) - (y - y2) * (x1 - x2));
  }
  expect(crossProducts[0]).toBeCloseTo(-crossProducts[1], 8);
  expect(crossProducts[0]).not.toBeCloseTo(0, 8);
});

test("scales the counting-line stroke and arrow for larger frames", () => {
  const { context } = canvasPage({ width: 900 });
  expect(context.lineWidth).toBeCloseTo(3.6, 8);
  for (const [x, y] of context.lineTo.mock.calls.slice(-2)) {
    expect(Math.hypot(x - 100, y - 120)).toBeCloseTo(20, 8);
  }
});

test.each([{ complete: false }, { naturalWidth: 0 }])("waits for a usable frame: %j", (options) => {
  const { image, context, instructions } = canvasPage(options);
  expect(context.drawImage).not.toHaveBeenCalled();
  Object.defineProperties(image, { complete: { value: true }, naturalWidth: { value: 320 } });
  fireEvent.load(image);
  expect(context.stroke).toHaveBeenCalledTimes(1);
  expect(instructions).toHaveTextContent("Frame ready. Your current line is preserved");
});

test("redraws edited coordinates but does not draw non-finite input", () => {
  const { context } = canvasPage();
  const x = screen.getByLabelText("X1");
  fireEvent.input(x, { target: { value: "40" } });
  expect(context.moveTo).toHaveBeenCalledWith(40, 20);
  context.stroke.mockClear();
  fireEvent.input(x, { target: { value: "not-a-number" } });
  expect(context.stroke).not.toHaveBeenCalled();
});

test("requests another preview time without losing the source and reports image errors", async () => {
  const { user, image, instructions } = canvasPage();
  const seconds = screen.getByLabelText("Preview time in seconds");
  await user.clear(seconds);
  await user.type(seconds, "3.5");
  const showFrame = screen.getByRole("button", { name: "Show frame" });
  const submitted = jest.fn();
  showFrame.form.addEventListener("submit", submitted);
  await user.click(showFrame);
  expect(submitted).toHaveBeenCalled();
  expect(submitted.mock.calls[0][0].defaultPrevented).toBe(true);
  const url = new URL(image.src);
  expect(url.pathname).toBe("/preview");
  expect(url.searchParams.get("video")).toBe("clip.mp4");
  expect(url.searchParams.get("seconds")).toBe("3.5");
  expect(instructions).toHaveTextContent("Loading preview frame...");
  expect(screen.getByLabelText("X1")).toHaveValue("10");
  fireEvent.error(image);
  expect(instructions).toHaveTextContent("Frame could not be loaded");
});

test("maps pointer positions to clamped image coordinates and resets the endpoint", async () => {
  const { user, canvas, context, instructions } = canvasPage();
  const point = (clientX, clientY) => user.pointer({
    target: canvas, keys: "[MouseLeft]", coords: { clientX, clientY },
  });
  context.stroke.mockClear();
  await point(74, 200);
  expect(context.stroke).toHaveBeenCalled();
  expect(screen.getByLabelText("X1")).toHaveValue("32");
  expect(screen.getByLabelText("Y1")).toHaveValue("90");
  expect(instructions).toHaveTextContent("Now click the second endpoint.");
  await point(1000, -50);
  expect(screen.getByLabelText("X2")).toHaveValue("319");
  expect(screen.getByLabelText("Y2")).toHaveValue("0");
  expect(instructions).toHaveTextContent("Line selected. Review and save the calibration.");
  await point(100, 100);
  await user.click(screen.getByRole("button", { name: "Redraw line" }));
  expect(canvas).toHaveFocus();
  expect(instructions).toHaveTextContent("Click the first endpoint, then the second.");
  await point(-100, 1000);
  expect(screen.getByLabelText("X1")).toHaveValue("0");
  expect(screen.getByLabelText("Y1")).toHaveValue("179");
});

test("allows processing waits only for explicit backfill submission and clears stale approval", async () => {
  const user = userEvent.setup();
  document.body.innerHTML = `
    <label for="cloud-action">Action</label>
    <select id="cloud-action"><option value="">Choose an action</option>
      <option value="publish">Upload/publish only</option><option value="submit">Publish and register explicit backfill work</option>
    </select>
    <div id="wait-options" hidden><label><input id="wait-processing" type="checkbox" checked>Wait for processing after submission</label></div>`;
  loadPortal();
  const action = screen.getByRole("combobox", { name: "Action" });
  const wait = screen.getByLabelText("Wait for processing after submission");
  expect(wait).toBeDisabled();
  expect(wait).not.toBeChecked();
  expect(wait).not.toBeVisible();
  await user.selectOptions(action, "submit");
  expect(screen.getByRole("checkbox", { name: "Wait for processing after submission" })).toBeVisible();
  expect(wait).toBeEnabled();
  await user.click(wait);
  expect(wait).toBeChecked();
  await user.selectOptions(action, "submit");
  expect(wait).toBeChecked();
  await user.selectOptions(action, "publish");
  expect(wait).toBeDisabled();
  expect(wait).not.toBeChecked();
  expect(wait).not.toBeVisible();
});

function readinessMarkup({ withLines = true, withGeometry = true, capture = "" } = {}) {
  return `<section id="review-readiness" data-readiness-url="/footage/readiness" data-video="lobby/clip.mp4">
    <p id="readiness-update-status" role="status">Saved properties</p>
    <button id="retry-readiness" hidden>Retry draft check</button>
    <div id="readiness-results"><p>Local checks passed</p></div>
  </section><input type="hidden" name="csrf" value="test-token">
  <label for="review-camera">Assign footage to camera</label>
  <select id="review-camera"><option value="north-1" selected>North 1</option><option value="north-2">North 2</option></select>
  <label for="capture-override">Capture time override</label><input id="capture-override" type="datetime-local" value="${capture}">
  ${withGeometry ? '<label for="geometry-action">Video line on reassignment</label><select id="geometry-action"><option value="">Choose</option><option value="keep">Keep</option><option value="clear">Clear</option></select>' : ""}
  ${withLines ? ["X1", "Y1", "X2", "Y2"].map((label, index) => {
    const name = ["counting_line_x1", "counting_line_y1", "counting_line_x2", "counting_line_y2"][index];
    return `<label for="${name}">${label}</label><input id="${name}" value="${[10, 20, 100, 120][index]}">`;
  }).join("") : ""}`;
}

function readinessPage(options) {
  document.body.innerHTML = readinessMarkup(options);
  loadPortal();
  return userEvent.setup({ advanceTimers: jest.advanceTimersByTime });
}

const draftResponse = (text = "Draft checks passed (not saved)") => ({
  ok: true, text: async () => `<p>${text}</p>`,
});

describe("live readiness previews", () => {
  beforeEach(() => {
    jest.useFakeTimers();
    fetch.mockResolvedValue(draftResponse());
  });
  afterEach(() => jest.useRealTimers());

  test.each(["X1", "Y1", "X2", "Y2"])("checks edits to %s without saving or replacing inputs", async (label) => {
    const user = readinessPage();
    const field = screen.getByLabelText(label);
    await user.clear(field);
    await user.type(field, "40");
    expect(screen.getByText("Local checks passed")).not.toBeVisible();
    expect(screen.getByRole("status")).toHaveTextContent("Checking unsaved values");
    expect(document.querySelector("#review-readiness")).toHaveAttribute("aria-busy", "true");
    expect(document.querySelector("#readiness-results")).toHaveAttribute("aria-hidden", "true");
    expect(screen.queryByRole("button", { name: "Retry draft check" })).not.toBeInTheDocument();
    expect(fetch).not.toHaveBeenCalled();
    await jest.advanceTimersByTimeAsync(349);
    expect(fetch).not.toHaveBeenCalled();
    await jest.advanceTimersByTimeAsync(1);
    expect(fetch).toHaveBeenCalledTimes(1);
    const [url, options] = fetch.mock.calls[0];
    expect(url).toBe("/footage/readiness");
    expect(options.method).toBe("POST");
    const body = Object.fromEntries(options.body);
    expect(body).toMatchObject({ csrf: "test-token", video: "lobby/clip.mp4", camera_id: "north-1", line_override: "yes" });
    expect(body[field.id]).toBe("40");
    expect(body).not.toHaveProperty("captured_at_utc");
    expect(field).toHaveValue("40");
    expect(screen.getByText("Draft checks passed (not saved)")).toBeVisible();
    expect(document.querySelector("#readiness-results")).toHaveAttribute("aria-hidden", "false");
    expect(screen.getByRole("status")).toHaveTextContent("Readiness updated for unsaved values");
    expect(document.querySelector("#review-readiness")).toHaveAttribute("aria-busy", "false");
  });

  test("checks camera and geometry choices and clearing a capture time without fabricating a line edit", async () => {
    const user = readinessPage({ withLines: false, capture: "2026-01-01T12:00" });
    await user.selectOptions(screen.getByLabelText("Assign footage to camera"), "north-2");
    await user.selectOptions(screen.getByLabelText("Video line on reassignment"), "clear");
    fireEvent.input(screen.getByLabelText("Capture time override"), { target: { value: "" } });
    await jest.advanceTimersByTimeAsync(350);
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(Object.fromEntries(fetch.mock.calls[0][1].body)).toEqual({
      csrf: "test-token", video: "lobby/clip.mp4", camera_id: "north-2",
      geometry_action: "clear", captured_at_utc: "",
    });
  });

  test.each([
    ["Assign footage to camera", "north-2", "camera_id"],
    ["Video line on reassignment", "keep", "geometry_action"],
  ])("checks an independent change to %s", async (label, value, key) => {
    const user = readinessPage();
    await user.selectOptions(screen.getByLabelText(label), value);
    await jest.advanceTimersByTimeAsync(350);
    expect(fetch).toHaveBeenCalledTimes(1);
    const body = Object.fromEntries(fetch.mock.calls[0][1].body);
    expect(body[key]).toBe(value);
    expect(body).not.toHaveProperty("line_override");
  });

  test("does not treat preview-only controls as footage metadata changes", async () => {
    readinessPage();
    document.body.insertAdjacentHTML("beforeend", '<label for="preview-only">Preview time</label><input id="preview-only">');
    fireEvent.input(screen.getByLabelText("Preview time"), { target: { value: "2" } });
    await jest.advanceTimersByTimeAsync(350);
    expect(fetch).not.toHaveBeenCalled();
    expect(screen.getByText("Local checks passed")).toBeVisible();
  });

  test("checks capture-time edits without an optional geometry choice", async () => {
    readinessPage({ withGeometry: false });
    fireEvent.input(screen.getByLabelText("Capture time override"), { target: { value: "2029-01-01T12:00" } });
    await jest.advanceTimersByTimeAsync(350);
    const body = Object.fromEntries(fetch.mock.calls[0][1].body);
    expect(body.captured_at_utc).toBe("2029-01-01T12:00");
    expect(body).not.toHaveProperty("line_override");
    expect(body).not.toHaveProperty("geometry_action");
  });

  test("checks calendar controls that emit only a change event", async () => {
    readinessPage();
    fireEvent.change(screen.getByLabelText("Capture time override"), { target: { value: "2028-01-01T12:00" } });
    await jest.advanceTimersByTimeAsync(350);
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(Object.fromEntries(fetch.mock.calls[0][1].body).captured_at_utc).toBe("2028-01-01T12:00");
  });

  test("stops probing after the current draft has been checked", async () => {
    const user = readinessPage();
    await user.selectOptions(screen.getByLabelText("Assign footage to camera"), "north-2");
    await jest.advanceTimersByTimeAsync(350);
    await jest.advanceTimersByTimeAsync(1000);
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(screen.getByText("Draft checks passed (not saved)")).toBeVisible();
  });

  test("keeps the readiness footprint stable while operators edit or draw", async () => {
    const user = readinessPage();
    const results = document.querySelector("#readiness-results");
    const status = document.querySelector("#readiness-update-status");
    const measure = jest.spyOn(results, "getBoundingClientRect").mockReturnValue({ height: 180 });
    jest.spyOn(status, "getBoundingClientRect").mockReturnValue({ height: 40 });
    await user.selectOptions(screen.getByLabelText("Assign footage to camera"), "north-2");
    expect(results.style.height).toBe("180px");
    await jest.advanceTimersByTimeAsync(350);
    expect(results.style.height).toBe("180px");
    expect(results.style.overflowY).toBe("auto");
    expect(status.style.minHeight).toBe("40px");
    await user.selectOptions(screen.getByLabelText("Assign footage to camera"), "north-1");
    await jest.advanceTimersByTimeAsync(350);
    expect(measure).toHaveBeenCalledTimes(1);
  });

  test.each([
    { failure: false, queuedTimerRan: false },
    { failure: true, queuedTimerRan: true },
  ])("coalesces in-flight edits and ignores stale results or errors (%j)", async ({ failure, queuedTimerRan }) => {
    let complete;
    fetch.mockImplementationOnce(() => new Promise((resolve, reject) => { complete = failure ? reject : resolve; }))
      .mockResolvedValueOnce(draftResponse("Newest draft result"));
    const user = readinessPage();
    const field = screen.getByLabelText("X1");
    await user.clear(field);
    await user.type(field, "40");
    await jest.advanceTimersByTimeAsync(350);
    await user.clear(field);
    await user.type(field, "50");
    if (queuedTimerRan) await jest.advanceTimersByTimeAsync(350);
    expect(fetch).toHaveBeenCalledTimes(1);
    complete(failure ? new Error("Stale failure") : draftResponse("Stale result"));
    await jest.advanceTimersByTimeAsync(0);
    expect(screen.queryByText("Stale result")).not.toBeInTheDocument();
    expect(screen.queryByText(/Stale failure/)).not.toBeInTheDocument();
    await jest.advanceTimersByTimeAsync(350);
    expect(fetch).toHaveBeenCalledTimes(2);
    expect(Object.fromEntries(fetch.mock.calls[1][1].body).counting_line_x1).toBe("50");
    expect(screen.getByText("Newest draft result")).toBeVisible();
    expect(field).toHaveValue("50");
  });

  test("keeps edits on failure and retries the current draft", async () => {
    fetch.mockResolvedValueOnce({ ok: false, status: 503 }).mockResolvedValueOnce(draftResponse());
    const user = readinessPage();
    await user.clear(screen.getByLabelText("X1"));
    await user.type(screen.getByLabelText("X1"), "40");
    await jest.advanceTimersByTimeAsync(350);
    expect(screen.getByRole("alert")).toHaveTextContent("HTTP 503");
    expect(screen.getByLabelText("X1")).toHaveValue("40");
    await user.click(screen.getByRole("button", { name: "Retry draft check" }));
    await jest.advanceTimersByTimeAsync(350);
    expect(fetch).toHaveBeenCalledTimes(2);
    expect(screen.getByText("Draft checks passed (not saved)")).toBeVisible();
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Retry draft check" })).not.toBeInTheDocument();
  });

  test("checks drawn endpoints as an unsaved video-line edit", async () => {
    const { canvas } = canvasPage({ extra: readinessMarkup({ withLines: false }) });
    const user = userEvent.setup({ advanceTimers: jest.advanceTimersByTime });
    await user.pointer({ target: canvas, keys: "[MouseLeft]", coords: { clientX: 74, clientY: 200 } });
    await jest.advanceTimersByTimeAsync(350);
    expect(fetch).toHaveBeenCalledTimes(1);
    const body = Object.fromEntries(fetch.mock.calls[0][1].body);
    expect(body.line_override).toBe("yes");
    expect(body.counting_line_x1).toBe("32");
    expect(body.counting_line_y1).toBe("90");
  });
});
