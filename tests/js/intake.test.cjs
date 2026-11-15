"use strict";
require("@testing-library/jest-dom");
const { configure, fireEvent, screen, waitFor } = require("@testing-library/dom");

configure({ asyncUtilTimeout: 250 });

let sent;
beforeEach(() => {
  jest.resetModules();
  jest.spyOn(window, "addEventListener");
  sent = [];
  global.fetch = jest.fn(async (url, options) => ({
    ok: true,
    json: async () => url.endsWith("/destination") ? { folder: options.body.get("folder") } :
      url.endsWith("/review") ? { exists: false } :
      url.endsWith("/uploads") ? { id: "upload-one", state: "uploading" } :
      url.endsWith("/finish") ? { id: "upload-one", state: "queued" } :
      url.endsWith("/cancel") ? { id: "upload-one", state: "cancelled" } :
      { id: "upload-one", state: "succeeded", sha256: "a".repeat(64) },
  }));
  global.fetch = new Proxy(global.fetch, {
    apply(target, receiver, args) {
      if (target.mock.calls.length > 50) throw new Error("Storage requests did not make progress");
      const options = args[1];
      const path = new URL(args[0], location.origin).pathname;
      const endpoint = path.split("/").pop();
      const posts = ["destination", "review", "uploads", "finish", "cancel", "folders", "move"];
      if (options && (!posts.includes(endpoint) || !path.startsWith("/intake/"))) {
        throw new Error("Storage endpoint not found");
      }
      if (!options && !/^\/intake\/operations\/[\w-]+$/.test(path)) throw new Error("Operation endpoint not found");
      if (options && (options.method !== "POST" || !(options.body instanceof FormData) || options.body.get("csrf") !== "csrf")) {
        throw new Error("Server rejected the request method or form token");
      }
      const fields = {
        destination: ["folder"], review: ["path"], uploads: ["folder", "name", "size"],
        folders: ["path"], move: ["paths", "destination", "rename", "replacements"],
      };
      if (options && (fields[endpoint] || []).some(key => options.body.get(key) === null)) {
        throw new Error("Server rejected missing operation parameters");
      }
      return Reflect.apply(target, receiver, args);
    },
  });
  global.XMLHttpRequest = class {
    upload = {};
    open(method, url) { this.method = method; this.url = url; }
    headers = {};
    setRequestHeader(name, value) { this.headers[name] = value; }
    send(data) {
      if (sent.length > 8) throw new Error("Upload did not make byte progress");
      if (this.method !== "PUT" || this.headers["X-CSRF-Token"] !== "csrf" || this.headers["Content-Type"] !== "application/octet-stream") {
        throw new Error("Server rejected the raw upload protocol");
      }
      if (!/^\/intake\/uploads\/[\w-]+\/chunks\?offset=\d+$/.test(this.url)) throw new Error("Upload endpoint not found");
      sent.push({ url: this.url, data, headers: this.headers });
      this.upload.onprogress({ lengthComputable: true, loaded: data.size });
      this.status = 200;
      this.responseText = JSON.stringify({ received: Number(new URL(this.url, location.origin).searchParams.get("offset")) + data.size });
      this.onload();
    }
    abort() { if (this.onabort) this.onabort(); }
  };
  document.body.innerHTML = `
  <div id="intake-explorer" data-upload-url="/intake/uploads" data-destination-url="/intake/destination" data-review-url="/intake/review"
    data-move-url="/intake/move" data-folder-url="/intake/folders" data-status-url="/intake/operations/OPERATION"
    data-download-url="/intake/download" data-max-upload-bytes="5368709120">
    <form data-upload-form><input name="csrf" value="csrf" type="hidden">
      <label>ADLS destination folder<input name="folder" value="north"></label><button type="submit">Confirm destination</button>
      <label>Choose local videos<input type="file" id="upload-files" accept=".mp4,.mov,.mkv,.avi" multiple disabled></label>
      <button type="button" data-upload-start disabled>Upload selected videos</button>
    </form><p data-intake-message role="status"></p><ul data-upload-results></ul>
    <button data-intake-action="move">Move selected</button><button data-intake-action="download">Download selected</button>
    <button data-intake-action="rename">Rename selected</button><button data-intake-action="create">Create folder</button>
    <dialog data-intake-dialog><form data-intake-dialog-form><h2 id="intake-dialog-title"></h2><p data-intake-dialog-detail></p>
      <div data-intake-dialog-field><label>Folder or new name<input name="value"></label></div>
      <label data-intake-confirm-field hidden><input name="confirm" type="checkbox">I approve replacing this exact destination version</label>
      <button type="button" data-intake-dialog-cancel>Cancel</button><button>Confirm</button>
    </form></dialog>
  </div>`;
  const dialog = document.querySelector("dialog");
  dialog.showModal = () => { dialog.open = true; };
  dialog.close = () => { dialog.open = false; dialog.dispatchEvent(new Event("close")); };
});
afterEach(() => {
  window.dispatchEvent(new Event("pagehide"));
  for (const args of window.addEventListener.mock.calls) window.removeEventListener(...args);
  jest.useRealTimers();
  delete global.fetch;
  jest.restoreAllMocks();
});

test("loads without optional intake controls", () => {
  document.body.innerHTML = "";
  load();
  expect(fetch).not.toHaveBeenCalled();
});

test("changing the destination requires confirmation again and rejects unsupported/empty files", async () => {
  load();
  await select(new File(["x"], "<img>.txt"));
  expect(screen.getByText("Unsupported video format.")).toBeVisible();
  expect(document.querySelector("img")).toBeNull();
  fireEvent.input(screen.getByLabelText("ADLS destination folder"), { target: { value: "south" } });
  expect(screen.getByLabelText("Choose local videos")).toBeDisabled();
  await select(new File([], "empty.mp4"));
  expect(screen.getByText(/File is empty/)).toBeVisible();
  expect(screen.getByRole("button", { name: "Upload selected videos" })).toBeDisabled();
});

test.each([null, [], "north", { folder: 123 }])("invalid destination replies remain errors (%j)", async body => {
  load();
  fetch.mockResolvedValue({ ok: true, json: async () => body });
  fireEvent.submit(document.querySelector("[data-upload-form]"));
  await screen.findByText(`Storage action failed: ${body && !Array.isArray(body) && typeof body === "object" ?
    "Invalid destination response" : "Invalid storage response"}`);
  expect(screen.getByLabelText("Choose local videos")).toBeDisabled();
});

test.each(["offset", "http", "json"])("chunk %s failures do not claim upload success", async mode => {
  load();
  await select(new File(["abc"], "clip.mp4"));
  global.XMLHttpRequest = class {
    upload = {};
    open() {}
    setRequestHeader() {}
    send() {
      this.status = mode === "http" ? 500 : 200;
      this.responseText = mode === "json" ? "invalid JSON" : JSON.stringify(mode === "offset" ? { received: 4 } : {});
      this.onload();
    }
  };
  fireEvent.click(screen.getByRole("button", { name: "Upload selected videos" }));
  await screen.findByText(/Upload failed:/);
  if (mode === "offset") expect(screen.getByText("Upload failed: Storage accepted an unexpected byte offset")).toBeVisible();
  expect(fetch.mock.calls.some(([url]) => url.endsWith("/finish"))).toBe(false);
});

test("cancelling a pending file sends no bytes or Azure upload request", async () => {
  load();
  await select(new File(["abc"], "clip.mp4"));
  fireEvent.click(screen.getByRole("button", { name: "Cancel upload" }));
  await screen.findByText(/Upload cancelled/);
  expect(screen.getByRole("button", { name: "Cancel upload" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "Retry" })).toBeVisible();
  expect(sent).toHaveLength(0);
  expect(fetch.mock.calls.some(([url]) => url.endsWith("/uploads"))).toBe(false);
});

test.each([false, true])("active upload cancellation aborts transfer and surfaces cleanup errors (%s)", async failure => {
  load();
  await select(new File(["abc"], "clip.mp4"));
  const Xhr = global.XMLHttpRequest;
  let active;
  global.XMLHttpRequest = class extends Xhr {
    send() { active = this; }
  };
  const normal = fetch.getMockImplementation();
  fetch.mockImplementation((url, options) => url.endsWith("/cancel") && failure ?
    Promise.resolve({ ok: false, status: 403, json: async () => ({ error: "Cleanup denied" }) }) : normal(url, options));
  fireEvent.click(screen.getByRole("button", { name: "Upload selected videos" }));
  await waitFor(() => expect(active).toBeDefined());
  fireEvent.click(screen.getByRole("button", { name: "Cancel upload" }));
  await screen.findByText(failure ? /Cleanup denied/ : /Upload cancelled/);
  await waitFor(() => expect(screen.getByRole("button", { name: "Confirm destination" })).toBeEnabled());
  expect(screen.getByRole("button", { name: "Cancel upload" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "Retry" })).toBeVisible();
  expect(fetch.mock.calls.some(([url]) => url.endsWith("/upload-one/cancel"))).toBe(true);
});

test.each(["cancel", "approve"])("existing destination replacement requires explicit confirmation (%s)", async choice => {
  load();
  await select(new File(["abc"], "clip.mp4"));
  const normal = fetch.getMockImplementation();
  fetch.mockImplementation((url, options) => url.endsWith("/review") ? Promise.resolve({
    ok: true, json: async () => ({ exists: true, path: "north/clip.mp4", size: 9, confirmation: "signed-review" }),
  }) : normal(url, options));
  fireEvent.click(screen.getByRole("button", { name: "Upload selected videos" }));
  await waitFor(() => expect(document.querySelector("dialog").open).toBe(true));
  if (choice === "cancel") {
    fireEvent.click(screen.getByRole("button", { name: "Cancel", exact: true }));
    await screen.findByText("Upload cancelled.");
    expect(sent).toHaveLength(0);
  } else {
    fireEvent.submit(document.querySelector("[data-intake-dialog-form]"));
    expect(screen.getByText(/Check the replacement confirmation/)).toBeVisible();
    fireEvent.click(screen.getByLabelText("I approve replacing this exact destination version"));
    fireEvent.submit(document.querySelector("[data-intake-dialog-form]"));
    await screen.findByText(/SHA-256/);
    const start = fetch.mock.calls.find(([url]) => url.endsWith("/uploads"));
    expect(start[1].body.get("confirmation")).toBe("signed-review");
    expect(start[1].body.get("confirm_replace")).toBe("yes");
  }
});

test.each(["invalid", "failed", "missing-hash"])("verification %s cannot be reported as a successful upload", async mode => {
  load();
  await select(new File(["abc"], "clip.mp4"));
  const normal = fetch.getMockImplementation();
  fetch.mockImplementation((url, options) => url.includes("/operations/") ? Promise.resolve({
    ok: true, json: async () => mode === "invalid" ? {} : mode === "failed" ?
      { state: "failed", message: "Source version changed" } : { state: "succeeded" },
  }) : normal(url, options));
  fireEvent.click(screen.getByRole("button", { name: "Upload selected videos" }));
  await screen.findByText(`Upload failed: ${mode === "invalid" ? "Invalid operation status" :
    mode === "failed" ? "Source version changed" : "Verified SHA-256 is missing"}`);
});

test("bulk download keeps source paths and rejects folders", async () => {
  load();
  const open = jest.spyOn(window, "open").mockImplementation(() => null);
  const box = document.createElement("input");
  box.type = "checkbox"; box.checked = true; box.value = "north/clip.mp4";
  box.dataset.intakeSelection = ""; box.dataset.directory = "false"; document.body.append(box);
  fireEvent.click(screen.getByRole("button", { name: "Download selected" }));
  await waitFor(() => expect(open).toHaveBeenCalled());
  expect(new URL(open.mock.calls[0][0]).searchParams.getAll("video")).toEqual(["north/clip.mp4"]);
  expect(open.mock.calls[0].slice(1)).toEqual(["_blank", "noopener"]);
  await waitFor(() => expect(screen.getByRole("button", { name: "Confirm destination" })).toBeEnabled());
  box.dataset.directory = "true";
  fireEvent.click(screen.getByRole("button", { name: "Download selected" }));
  await screen.findByText(/Select supported videos/);
  expect(open).toHaveBeenCalledTimes(1);
});

test("moving checked files reviews the target and queues server-side move without discarding local edits", async () => {
  load();
  const box = document.createElement("input");
  box.type = "checkbox"; box.checked = true; box.value = "north/clip.mp4";
  box.dataset.intakeSelection = ""; box.dataset.directory = "false"; document.body.append(box);
  fireEvent.click(screen.getByRole("button", { name: "Move selected" }));
  expect(document.querySelector("dialog").open).toBe(true);
  fireEvent.change(screen.getByLabelText("Folder or new name"), { target: { value: "archive" } });
  fireEvent.submit(document.querySelector("[data-intake-dialog-form]"));
  await screen.findByText(/Move completed/);
  const move = fetch.mock.calls.find(([url]) => url.endsWith("/move"));
  expect(move[1].body.getAll("paths")).toEqual(["north/clip.mp4"]);
  expect(move[1].body.get("destination")).toBe("archive");
  expect(screen.getByLabelText("ADLS destination folder")).toHaveValue("north");
});

function checked(path, directory = false) {
  const box = document.createElement("input");
  box.type = "checkbox"; box.checked = true; box.value = path;
  box.dataset.intakeSelection = ""; box.dataset.directory = String(directory);
  document.body.append(box);
  return box;
}

test.each([false, true])("creating a folder is explicit and cancellation changes nothing (%s)", async cancel => {
  load();
  fireEvent.click(screen.getByRole("button", { name: "Create folder" }));
  expect(screen.getByText("Create folder", { selector: "h2" })).toBeVisible();
  if (cancel) fireEvent.click(screen.getByRole("button", { name: "Cancel", exact: true }));
  else {
    fireEvent.change(screen.getByLabelText("Folder or new name"), { target: { value: "new/site" } });
    fireEvent.submit(document.querySelector("[data-intake-dialog-form]"));
    await screen.findByText(/Folder created/);
  }
  expect(fetch.mock.calls.filter(([url]) => url.endsWith("/folders"))).toHaveLength(cancel ? 0 : 1);
});

test.each(["move", "rename", "download"])("empty %s selection is rejected without storage changes", async kind => {
  load();
  fireEvent.click(document.querySelector(`[data-intake-action="${kind}"]`));
  await screen.findByText(/Storage action failed:/);
  expect(fetch).not.toHaveBeenCalled();
});

test("renaming one folder queues a native move; multiple selected items are rejected", async () => {
  load();
  checked("north", true);
  fireEvent.click(screen.getByRole("button", { name: "Rename selected" }));
  fireEvent.change(screen.getByLabelText("Folder or new name"), { target: { value: "south" } });
  fireEvent.submit(document.querySelector("[data-intake-dialog-form]"));
  await screen.findByText(/Move completed/);
  const move = fetch.mock.calls.find(([url]) => url.endsWith("/move"));
  expect(move[1].body.get("rename")).toBe("south");
  expect(fetch.mock.calls.some(([url]) => url.endsWith("/review"))).toBe(false);
  await waitFor(() => expect(screen.getByRole("button", { name: "Confirm destination" })).toBeEnabled());
  checked("another", true);
  fireEvent.click(screen.getByRole("button", { name: "Rename selected" }));
  await screen.findByText(/exactly one item/);
  expect(fetch.mock.calls.filter(([url]) => url.endsWith("/move"))).toHaveLength(1);
});

test("cancelling a move or a move replacement makes no move request", async () => {
  load();
  checked("north/clip.mp4");
  fireEvent.click(screen.getByRole("button", { name: "Move selected" }));
  fireEvent.click(screen.getByRole("button", { name: "Cancel", exact: true }));
  await waitFor(() => expect(screen.getByRole("button", { name: "Confirm destination" })).toBeEnabled());
  fetch.mockResolvedValue({ ok: true, json: async () => ({ exists: true, path: "archive/clip.mp4", size: 3, confirmation: "review" }) });
  fireEvent.click(screen.getByRole("button", { name: "Move selected" }));
  fireEvent.change(screen.getByLabelText("Folder or new name"), { target: { value: "archive" } });
  fireEvent.submit(document.querySelector("[data-intake-dialog-form]"));
  await screen.findByText("Replace existing video?", { selector: "h2" });
  fireEvent.click(screen.getByRole("button", { name: "Cancel", exact: true }));
  expect(fetch.mock.calls.some(([url]) => url.endsWith("/move"))).toBe(false);
});

test.each([null, "", 42])("invalid upload identity stops before bytes are sent (%j)", async id => {
  load();
  await select(new File(["abc"], "clip.mp4"));
  const normal = fetch.getMockImplementation();
  fetch.mockImplementation((url, options) => url.endsWith("/uploads") ? Promise.resolve({ ok: true, json: async () => ({ id }) }) : normal(url, options));
  fireEvent.click(screen.getByRole("button", { name: "Upload selected videos" }));
  await screen.findByText(/Invalid upload identity/);
  expect(sent).toHaveLength(0);
});

test("active transfers warn on leaving and send bounded cancellation even if cleanup fails", async () => {
  load();
  await select(new File(["abc"], "clip.mp4"));
  const Xhr = global.XMLHttpRequest;
  let active;
  global.XMLHttpRequest = class extends Xhr { send() { active = this; } };
  const normal = fetch.getMockImplementation();
  fetch.mockImplementation((url, options) => url.endsWith("/cancel") ?
    Promise.resolve({ ok: false, status: 403, json: async () => ({ error: "Cleanup failed" }) }) : normal(url, options));
  fireEvent.click(screen.getByRole("button", { name: "Upload selected videos" }));
  await waitFor(() => expect(active).toBeDefined());
  const event = new Event("beforeunload", { cancelable: true });
  window.dispatchEvent(event);
  expect(event.defaultPrevented).toBe(true);
  const abort = jest.spyOn(active, "abort");
  window.dispatchEvent(new Event("pagehide"));
  await screen.findByText(/Cannot stop unfinished upload/);
  expect(abort).toHaveBeenCalledTimes(1);
  const cancel = fetch.mock.calls.find(([url]) => url.endsWith("/cancel"));
  expect(cancel[1].keepalive).toBe(true);
  expect(sent).toHaveLength(0);
});

test("pending verification polls until completion and stops when the page closes", async () => {
  jest.useFakeTimers();
  load();
  await select(new File(["abc"], "clip.mp4"));
  const normal = fetch.getMockImplementation();
  let polled = 0;
  fetch.mockImplementation((url, options) => url.includes("/operations/") ? Promise.resolve({
    ok: true, json: async () => ++polled === 1 ? { state: "verifying", message: "Checking source" } :
      { state: "succeeded", sha256: "a".repeat(64) },
  }) : normal(url, options));
  fireEvent.click(screen.getByRole("button", { name: "Upload selected videos" }));
  await jest.advanceTimersByTimeAsync(0);
  await screen.findByText("Checking source");
  await jest.advanceTimersByTimeAsync(1000);
  await screen.findByText(/SHA-256/);
  expect(polled).toBe(2);
  jest.useRealTimers();
});

test("an upload from the allowed root labels its destination without inventing a local source", async () => {
  load();
  screen.getByLabelText("ADLS destination folder").value = "";
  await select(new File(["abc"], "clip.mp4"));
  expect(screen.getByText("clip.mp4 -> allowed root")).toBeVisible();
});

test("HTTP failures without a server message remain explicit and do not enable selection", async () => {
  load();
  fetch.mockResolvedValue({ ok: false, status: 413, json: async () => ({}) });
  fireEvent.submit(document.querySelector("[data-upload-form]"));
  await screen.findByText(/HTTP 413/);
  expect(screen.getByLabelText("Choose local videos")).toBeDisabled();
});

test("another bulk action cannot open a second confirmation while one is pending", async () => {
  load();
  checked("clip.mp4");
  fireEvent.click(screen.getByRole("button", { name: "Move selected" }));
  fireEvent.click(screen.getByRole("button", { name: "Create folder" }));
  expect(screen.getByText(/Wait for the current transfer or confirmation/)).toBeVisible();
  expect(screen.getByText("Move selected items", { selector: "h2" })).toBeVisible();
  fireEvent.click(screen.getByRole("button", { name: "Cancel", exact: true }));
});

test("move can target the allowed root; rename still requires a name", async () => {
  load();
  checked("north/clip.mp4");
  fireEvent.click(screen.getByRole("button", { name: "Move selected" }));
  expect(screen.getByLabelText("Folder or new name")).not.toBeRequired();
  fireEvent.submit(document.querySelector("[data-intake-dialog-form]"));
  await screen.findByText(/Move completed/);
  const move = fetch.mock.calls.find(([url]) => url.endsWith("/move"));
  expect(move[1].body.get("destination")).toBe("");
  await waitFor(() => expect(screen.getByRole("button", { name: "Confirm destination" })).toBeEnabled());
  fireEvent.click(screen.getByRole("button", { name: "Rename selected" }));
  expect(screen.getByLabelText("Folder or new name")).toBeRequired();
  fireEvent.submit(document.querySelector("[data-intake-dialog-form]"));
  expect(screen.getByText(/Enter a new name/)).toBeVisible();
  expect(fetch.mock.calls.filter(([url]) => url.endsWith("/move"))).toHaveLength(1);
});

test("a failed staging creation preserves its operation ID and error without sending chunks", async () => {
  load();
  await select(new File(["abc"], "clip.mp4"));
  const normal = fetch.getMockImplementation();
  fetch.mockImplementation((url, options) => url.endsWith("/uploads") ?
    Promise.resolve({ ok: true, json: async () => ({ id: "upload-one", state: "recovery", message: "Storage creation denied" }) }) : normal(url, options));
  fireEvent.click(screen.getByRole("button", { name: "Upload selected videos" }));
  await screen.findByText(/Storage creation denied/);
  expect(sent).toHaveLength(0);
});

test("recovery after cancellation is not reported as a successful cancellation", async () => {
  load();
  await select(new File(["abc"], "clip.mp4"));
  const Xhr = global.XMLHttpRequest;
  let active;
  global.XMLHttpRequest = class extends Xhr { send() { active = this; } };
  const normal = fetch.getMockImplementation();
  fetch.mockImplementation((url, options) => url.endsWith("/cancel") ?
    Promise.resolve({ ok: true, json: async () => ({ state: "recovery", message: "Finalization already began" }) }) : normal(url, options));
  fireEvent.click(screen.getByRole("button", { name: "Upload selected videos" }));
  await waitFor(() => expect(active).toBeDefined());
  fireEvent.click(screen.getByRole("button", { name: "Cancel upload" }));
  await screen.findByText(/Finalization already began/);
  expect(screen.queryByText("Upload cancelled; retry starts this file again.")).not.toBeInTheDocument();
});

function load() { require("../../src/people_counter/portal_static/portal_intake.js"); }
async function select(file) {
  fireEvent.submit(document.querySelector("[data-upload-form]"));
  await waitFor(() => expect(screen.getByLabelText("Choose local videos")).toBeEnabled());
  fireEvent.change(screen.getByLabelText("Choose local videos"), { target: { files: [file] } });
}

test("destination is confirmed before local selection; upload uses bounded raw chunks and reports verified hash", async () => {
  load();
  expect(screen.getByLabelText("Choose local videos")).toBeDisabled();
  await select(new File([new Uint8Array(8 * 1024 * 1024 + 3)], "clip.mp4"));
  fireEvent.click(screen.getByRole("button", { name: "Upload selected videos" }));
  await screen.findByText(/SHA-256/);
  expect(sent.map(row => row.data.size)).toEqual([8 * 1024 * 1024, 3]);
  expect(sent[0].headers["X-CSRF-Token"]).toBe("csrf");
  expect(sent[0].headers["Content-Type"]).toBe("application/octet-stream");
  const start = fetch.mock.calls.find(([url]) => url.endsWith("/uploads"));
  expect(start[1].body.get("folder")).toBe("north");
  expect(start[1].body.get("size")).toBe(String(8 * 1024 * 1024 + 3));
  expect(document.querySelector("progress").value).toBe(8 * 1024 * 1024 + 3);
  expect(screen.getByRole("progressbar", { name: "Upload progress for clip.mp4" })).toHaveAttribute("max", String(8 * 1024 * 1024 + 3));
});

test("oversized videos are rejected before Azure upload begins", async () => {
  load();
  const file = new File(["x"], "huge.mp4");
  Object.defineProperty(file, "size", { value: 5 * 1024**3 + 1 });
  await select(file);
  expect(await screen.findByText(/exceeds the configured/)).toBeVisible();
  expect(sent).toHaveLength(0);
  expect(fetch.mock.calls.filter(([url]) => url.endsWith("/uploads"))).toHaveLength(0);
});

test("server upload errors are visible and a retry restarts only that file at its original destination", async () => {
  load();
  await select(new File(["abc"], "clip.mp4"));
  const first = fetch.getMockImplementation();
  let failed = false;
  fetch.mockImplementation(async (url, options) => {
    if (url.endsWith("/uploads") && !failed) {
      failed = true;
      return { ok: false, status: 403, json: async () => ({ error: "Permission denied" }) };
    }
    return first(url, options);
  });
  fireEvent.click(screen.getByRole("button", { name: "Upload selected videos" }));
  await screen.findByText(/Permission denied/);
  await waitFor(() => expect(screen.getByRole("button", { name: "Retry" })).toBeEnabled());
  screen.getByLabelText("ADLS destination folder").value = "other";
  fireEvent.click(screen.getByRole("button", { name: "Retry" }));
  await screen.findByText(/SHA-256/);
  const starts = fetch.mock.calls.filter(([url]) => url.endsWith("/uploads"));
  expect(starts).toHaveLength(2);
  expect(starts.map(([, options]) => options.body.get("folder"))).toEqual(["north", "north"]);
  expect(sent).toHaveLength(1);
});

test("leaving while upload creation is in flight cancels the newly returned staging session", async () => {
  load();
  await select(new File(["abc"], "clip.mp4"));
  const normal = fetch.getMockImplementation();
  let resolveStart;
  fetch.mockImplementation((url, options) => url.endsWith("/uploads") ?
    new Promise(resolve => { resolveStart = resolve; }) : normal(url, options));
  fireEvent.click(screen.getByRole("button", { name: "Upload selected videos" }));
  await waitFor(() => expect(resolveStart).toBeDefined());
  window.dispatchEvent(new Event("pagehide"));
  resolveStart({ ok: true, json: async () => ({ id: "late-start", state: "uploading" }) });
  await waitFor(() => expect(fetch.mock.calls.some(([url]) => url.endsWith("/late-start/cancel"))).toBe(true));
  expect(sent).toHaveLength(0);
});

test("a failed chunk is cleaned up before retry starts a fresh upload", async () => {
  load();
  await select(new File(["abc"], "clip.mp4"));
  const Xhr = global.XMLHttpRequest;
  global.XMLHttpRequest = class extends Xhr {
    send() { this.onerror(); }
  };
  fireEvent.click(screen.getByRole("button", { name: "Upload selected videos" }));
  await screen.findByText(/Upload connection failed/);
  await waitFor(() => expect(screen.getByRole("button", { name: "Retry" })).toBeEnabled());
  global.XMLHttpRequest = Xhr;
  fireEvent.click(screen.getByRole("button", { name: "Retry" }));
  await screen.findByText(/SHA-256/);
  const calls = fetch.mock.calls.map(([url]) => url);
  const first = calls.indexOf("/intake/uploads");
  const cancelled = calls.indexOf("/intake/operations/upload-one/cancel");
  const next = calls.lastIndexOf("/intake/uploads");
  expect(cancelled).toBeGreaterThan(first);
  expect(cancelled).toBeLessThan(next);
});

test("a mixed batch uploads only pending files and keeps completed files out of later batches", async () => {
  load();
  await select(new File(["abc"], "first.MP4"));
  fireEvent.change(screen.getByLabelText("Choose local videos"), {
    target: { files: [new File(["bad"], "skip.txt"), new File(["defg"], "second.mov")] },
  });
  expect(screen.getAllByText("Ready to upload to the confirmed ADLS destination.")).toHaveLength(2);
  expect(screen.getByText("Destination confirmed. Choose local videos, then start uploading.")).toBeVisible();
  const rows = document.querySelectorAll("[data-upload-results] li");
  expect(rows[1].querySelector("button")).toBeDisabled();
  expect(rows[0].querySelector("p[role=status]")).toHaveTextContent("Ready to upload");
  for (const button of rows[0].querySelectorAll("button")) {
    expect(button).toHaveAttribute("type", "button");
    expect(button).toHaveClass("btn-secondary");
  }
  const start = screen.getByRole("button", { name: "Upload selected videos" });
  expect(start).toBeEnabled();
  fireEvent.click(start);
  await screen.findByText("Batch finished. Completed files remain in ADLS; retry only failed files. Refresh the folder to select uploaded footage.");
  expect(fetch.mock.calls.filter(([url]) => url.endsWith("/uploads")).map(([, o]) => Object.fromEntries(o.body))).toEqual([
    { csrf: "csrf", folder: "north", name: "first.MP4", size: "3", confirmation: "", confirm_replace: "yes" },
    { csrf: "csrf", folder: "north", name: "second.mov", size: "4", confirmation: "", confirm_replace: "yes" },
  ]);
  expect(sent.map(row => row.data.size)).toEqual([3, 4]);
  expect(rows[0].querySelector("progress")).toHaveValue(3);
  expect(rows[0].querySelector("button")).toBeDisabled();
  expect(rows[0].querySelectorAll("button")[1]).not.toBeVisible();
  expect(start).toBeDisabled();
  fireEvent.change(screen.getByLabelText("Choose local videos"), { target: { files: [new File(["x"], "third.avi")] } });
  fireEvent.click(start);
  await waitFor(() => expect(fetch.mock.calls.filter(([url]) => url.endsWith("/uploads"))).toHaveLength(3));
  await waitFor(() => expect(screen.getAllByText(`Uploaded. SHA-256: ${"a".repeat(64)}`)).toHaveLength(3));
  await waitFor(() => expect(screen.getByRole("button", { name: "Confirm destination" })).toBeEnabled());
  expect(start).toBeDisabled();
  expect(sent).toHaveLength(3);
  const leave = new Event("beforeunload", { cancelable: true });
  window.dispatchEvent(leave);
  expect(leave.defaultPrevented).toBe(false);
  window.dispatchEvent(new Event("pagehide"));
  expect(fetch.mock.calls.filter(([url]) => url.endsWith("/cancel"))).toHaveLength(0);
});

test("the configured inclusive size limit and empty-file progress remain usable", async () => {
  document.querySelector("#intake-explorer").dataset.maxUploadBytes = "3";
  load();
  await select(new File(["abc"], "exact.mp4"));
  fireEvent.change(screen.getByLabelText("Choose local videos"), { target: { files: [new File([], "empty.mp4")] } });
  expect(screen.getByRole("progressbar", { name: "Upload progress for empty.mp4" })).toHaveAttribute("max", "1");
  fireEvent.click(screen.getByRole("button", { name: "Upload selected videos" }));
  await screen.findByText(`Uploaded. SHA-256: ${"a".repeat(64)}`);
  expect(sent).toHaveLength(1);
});

test("folder dialog preserves focus, resets fields, and prevents native form navigation", async () => {
  load();
  const button = screen.getByRole("button", { name: "Create folder" });
  button.focus();
  fireEvent.click(button);
  const value = screen.getByLabelText("Folder or new name");
  const confirm = screen.getByLabelText("I approve replacing this exact destination version");
  expect(screen.getByText("Enter a new folder path within the configured ADLS root.")).toBeVisible();
  expect(value).toBeVisible();
  expect(value).toBeRequired();
  expect(confirm).not.toBeVisible();
  expect(confirm).not.toBeRequired();
  fireEvent.change(value, { target: { value: "  " } });
  expect(fireEvent.submit(document.querySelector("[data-intake-dialog-form]"))).toBe(false);
  expect(screen.getByText("Enter a new name or folder path before continuing.")).toBeVisible();
  expect(document.querySelector("dialog").open).toBe(true);
  expect(fetch).not.toHaveBeenCalled();
  fireEvent.change(value, { target: { value: "north/new" } });
  fireEvent.submit(document.querySelector("[data-intake-dialog-form]"));
  await screen.findByText("Folder created. Refresh the folder to view it.");
  expect(fetch).toHaveBeenCalledWith("/intake/folders", expect.objectContaining({ method: "POST" }));
  expect(fetch.mock.calls[0][1].body.get("path")).toBe("north/new");
  expect(button).toHaveFocus();
  fireEvent.click(button);
  expect(value).toHaveValue("");
  expect(confirm).not.toBeChecked();
  window.dispatchEvent(new Event("pagehide"));
  expect(document.querySelector("dialog").open).toBe(false);
  await waitFor(() => expect(screen.getByRole("button", { name: "Confirm destination" })).toBeEnabled());
  expect(fetch).toHaveBeenCalledTimes(1);
});

test.each(["rename", "move"])("%s preserves every selected source and signed replacement", async kind => {
  load();
  checked("north/camera/clip.mp4");
  if (kind === "move") {
    checked("south/other.mov");
    checked("north/folder", true);
  }
  const normal = fetch.getMockImplementation();
  fetch.mockImplementation((url, options) => url.endsWith("/review") ? Promise.resolve({
    ok: true, json: async () => ({
      exists: true, path: options.body.get("path"), size: 9, confirmation: `signed:${options.body.get("path")}`,
    }),
  }) : normal(url, options));
  fireEvent.click(document.querySelector(`[data-intake-action="${kind}"]`));
  expect(screen.getByText(kind === "rename" ? "Rename selected item" : "Move selected items", { selector: "h2" })).toBeVisible();
  expect(document.querySelector("[data-intake-dialog-detail]")).toHaveTextContent(
    kind === "rename" ? "north/camera/clip.mp4" : "north/camera/clip.mp4, south/other.mov, north/folder",
  );
  fireEvent.change(screen.getByLabelText("Folder or new name"), { target: { value: kind === "rename" ? "renamed.mp4" : "archive/deep" } });
  fireEvent.submit(document.querySelector("[data-intake-dialog-form]"));
  const targets = kind === "rename" ? ["north/camera/renamed.mp4"] : ["archive/deep/clip.mp4", "archive/deep/other.mov"];
  for (const path of targets) {
    await screen.findByText(`${path}: 9 bytes. This replaces the reviewed version and invalidates old recording overrides.`);
    expect(screen.getByLabelText("Folder or new name")).not.toBeVisible();
    expect(screen.getByLabelText("Folder or new name")).not.toBeRequired();
    const approval = screen.getByLabelText("I approve replacing this exact destination version");
    expect(approval).toBeRequired();
    expect(approval).not.toBeChecked();
    fireEvent.click(approval);
    fireEvent.submit(document.querySelector("[data-intake-dialog-form]"));
  }
  await screen.findByText("Move completed. Editable references updated; refresh the folder to view the new paths.");
  const move = fetch.mock.calls.find(([url]) => url.endsWith("/move"))[1].body;
  expect(move.getAll("paths")).toEqual(kind === "rename" ? ["north/camera/clip.mp4"] : ["north/camera/clip.mp4", "south/other.mov", "north/folder"]);
  expect(move.get("destination")).toBe(kind === "rename" ? "north/camera" : "archive/deep");
  expect(move.get("rename")).toBe(kind === "rename" ? "renamed.mp4" : "");
  expect(move.get("confirm_replace")).toBe("yes");
  expect(JSON.parse(move.get("replacements"))).toEqual(Object.fromEntries(targets.map(path => [path, `signed:${path}`])));
  expect(fetch.mock.calls.filter(([url]) => url.endsWith("/review")).map(([, o]) => o.body.get("path"))).toEqual(targets);
});

test("download encodes multiple checked videos, not unchecked sources", async () => {
  load();
  const open = jest.spyOn(window, "open").mockImplementation(() => null);
  checked("north/a & b.mp4");
  checked("south/other.mov");
  checked("not-selected.mp4").checked = false;
  fireEvent.click(screen.getByRole("button", { name: "Download selected" }));
  await waitFor(() => expect(open).toHaveBeenCalledTimes(1));
  const url = new URL(open.mock.calls[0][0]);
  expect(url.origin).toBe(location.origin);
  expect(url.pathname).toBe("/intake/download");
  expect(url.searchParams.getAll("video")).toEqual(["north/a & b.mp4", "south/other.mov"]);
});

test.each(["failed", "cancelled", "recovery", "cleanup_pending"])("verification %s uses the exact terminal state when no message is supplied", async state => {
  load();
  await select(new File(["abc"], "clip.mp4"));
  const normal = fetch.getMockImplementation();
  fetch.mockImplementation((url, options) => url.includes("/operations/") ?
    Promise.resolve({ ok: true, json: async () => ({ state }) }) : normal(url, options));
  fireEvent.click(screen.getByRole("button", { name: "Upload selected videos" }));
  await screen.findByText(`Upload failed: ${state}`);
  expect(screen.getByRole("button", { name: "Cancel upload" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "Retry" })).toBeVisible();
});

test.each(["COMPLETED", "succeeded"])("verification accepts %s and shows a fallback while still verifying", async state => {
  jest.useFakeTimers();
  load();
  await select(new File(["abc"], "clip.mp4"));
  const normal = fetch.getMockImplementation();
  let polls = 0;
  fetch.mockImplementation((url, options) => url.includes("/operations/") ? Promise.resolve({
    ok: true, json: async () => ++polls === 1 ? { state: "verifying" } : { state, sha256: "verified" },
  }) : normal(url, options));
  fireEvent.click(screen.getByRole("button", { name: "Upload selected videos" }));
  await jest.advanceTimersByTimeAsync(0);
  await screen.findByText("Verifying complete ADLS bytes...");
  expect(polls).toBe(1);
  await jest.advanceTimersByTimeAsync(999);
  expect(polls).toBe(1);
  await jest.advanceTimersByTimeAsync(1);
  await screen.findByText("Uploaded. SHA-256: verified");
});

test.each(["completed", "succeeded", "recovery"])("retry never recreates a staging upload after cancellation returns %s", async state => {
  load();
  await select(new File(["abc"], "clip.mp4"));
  const normal = fetch.getMockImplementation();
  fetch.mockImplementation((url, options) => url.endsWith("/cancel") ?
    Promise.resolve({ ok: true, json: async () => ({ state }) }) : normal(url, options));
  const Xhr = global.XMLHttpRequest;
  global.XMLHttpRequest = class extends Xhr { send() { this.onerror(); } };
  fireEvent.click(screen.getByRole("button", { name: "Upload selected videos" }));
  await screen.findByText("Upload failed: Upload connection failed");
  await waitFor(() => expect(screen.getByRole("button", { name: "Retry" })).toBeEnabled());
  fireEvent.click(screen.getByRole("button", { name: "Retry" }));
  await screen.findByText(state === "recovery" ?
    "Upload failed: Cancellation requires storage recovery." :
    "Upload failed: This upload already completed; refresh the folder instead of retrying.");
  expect(fetch.mock.calls.filter(([url]) => url.endsWith("/uploads"))).toHaveLength(1);
});

test.each(["completed", "succeeded"])("cancel racing finalization reports %s rather than a retryable cancellation", async state => {
  jest.useFakeTimers();
  load();
  await select(new File(["abc"], "clip.mp4"));
  const normal = fetch.getMockImplementation();
  fetch.mockImplementation((url, options) => url.endsWith("/cancel") ?
    Promise.resolve({ ok: true, json: async () => ({ state, sha256: "final-hash" }) }) :
    url.includes("/operations/") ? Promise.resolve({ ok: true, json: async () => ({ state: "verifying" }) }) :
      normal(url, options));
  fireEvent.click(screen.getByRole("button", { name: "Upload selected videos" }));
  await jest.advanceTimersByTimeAsync(0);
  await screen.findByText("Verifying complete ADLS bytes...");
  fireEvent.click(screen.getByRole("button", { name: "Cancel upload" }));
  await screen.findByText("Upload already completed. SHA-256: final-hash");
  expect(screen.getByRole("button", { name: "Cancel upload" })).toBeDisabled();
  expect(screen.getByText("Retry", { selector: "button" })).not.toBeVisible();
  window.dispatchEvent(new Event("pagehide"));
  await jest.advanceTimersByTimeAsync(1000);
});

test.each(["cancel", "pagehide"])("%s during verification stops polling and never cancels a finished byte stream", async kind => {
  jest.useFakeTimers();
  load();
  await select(new File(["abc"], "clip.mp4"));
  const normal = fetch.getMockImplementation();
  fetch.mockImplementation((url, options) => url.includes("/operations/") && !url.endsWith("/cancel") ?
    Promise.resolve({ ok: true, json: async () => ({ state: "verifying" }) }) : normal(url, options));
  fireEvent.click(screen.getByRole("button", { name: "Upload selected videos" }));
  await jest.advanceTimersByTimeAsync(0);
  await screen.findByText("Verifying complete ADLS bytes...");
  const leave = new Event("beforeunload", { cancelable: true });
  window.dispatchEvent(leave);
  expect(leave.defaultPrevented).toBe(false);
  if (kind === "cancel") {
    fireEvent.click(screen.getByRole("button", { name: "Cancel upload" }));
    await screen.findByText("Upload cancelled; retry starts this file again.");
  } else window.dispatchEvent(new Event("pagehide"));
  await jest.advanceTimersByTimeAsync(1000);
  await screen.findByText("Upload failed: Transfer cancelled; queued verification jobs remain in Jobs.");
  expect(fetch.mock.calls.filter(([url]) => /operations\/[^/]+$/.test(url))).toHaveLength(1);
  expect(fetch.mock.calls.filter(([url]) => url.endsWith("/cancel"))).toHaveLength(kind === "cancel" ? 1 : 0);
});

test.each(["cancel", "pagehide"])("%s during staging creation cancels the returned operation with the correct lifetime", async kind => {
  load();
  await select(new File(["abc"], "clip.mp4"));
  const normal = fetch.getMockImplementation();
  let resolveStart;
  fetch.mockImplementation((url, options) => url.endsWith("/uploads") ?
    new Promise(resolve => { resolveStart = resolve; }) : normal(url, options));
  fireEvent.click(screen.getByRole("button", { name: "Upload selected videos" }));
  await waitFor(() => expect(resolveStart).toBeDefined());
  expect(screen.getByLabelText("ADLS destination folder")).toHaveAttribute("readonly");
  expect(screen.getByLabelText("Choose local videos")).toBeDisabled();
  expect(screen.getByText("Retry", { selector: "button" })).toBeDisabled();
  if (kind === "cancel") fireEvent.click(screen.getByRole("button", { name: "Cancel upload" }));
  else window.dispatchEvent(new Event("pagehide"));
  resolveStart({ ok: true, json: async () => ({ id: "late-start", state: "uploading" }) });
  await screen.findByText("Upload failed: Upload cancelled");
  const cancel = fetch.mock.calls.find(([url]) => url.endsWith("/late-start/cancel"));
  expect(cancel[1].keepalive).toBe(kind === "pagehide");
  expect(sent).toHaveLength(0);
  expect(screen.getByRole("button", { name: "Retry" })).toBeVisible();
});

test("page closure while reviewing a batch stops later files and closes replacement confirmation", async () => {
  load();
  await select(new File(["abc"], "first.mp4"));
  fireEvent.change(screen.getByLabelText("Choose local videos"), { target: { files: [new File(["x"], "second.mp4")] } });
  const normal = fetch.getMockImplementation();
  let review;
  fetch.mockImplementation((url, options) => url.endsWith("/review") ?
    new Promise(resolve => { review = resolve; }) : normal(url, options));
  fireEvent.click(screen.getByRole("button", { name: "Upload selected videos" }));
  await waitFor(() => expect(review).toBeDefined());
  window.dispatchEvent(new Event("pagehide"));
  review({ ok: true, json: async () => ({ exists: true, path: "north/first.mp4", size: 3, confirmation: "signed" }) });
  await screen.findByText("Upload cancelled.");
  await waitFor(() => expect(screen.getByRole("button", { name: "Confirm destination" })).toBeEnabled());
  expect(document.querySelector("dialog").open).toBe(false);
  expect(fetch.mock.calls.filter(([url]) => url.endsWith("/review"))).toHaveLength(1);
  expect(fetch.mock.calls.filter(([url]) => url.endsWith("/uploads"))).toHaveLength(0);
});

test.each(["missing-confirmation", "creation-recovery", "bad-status"])("%s remains an explicit protocol error", async mode => {
  load();
  await select(new File(["abc"], "clip.mp4"));
  const normal = fetch.getMockImplementation();
  fetch.mockImplementation((url, options) =>
    mode === "missing-confirmation" && url.endsWith("/review") ? Promise.resolve({
      ok: true, json: async () => ({ exists: true, path: "north/clip.mp4", size: 3 }),
    }) : mode === "creation-recovery" && url.endsWith("/uploads") ? Promise.resolve({
      ok: true, json: async () => ({ id: "upload-one", state: "recovery" }),
    }) : mode === "bad-status" && url.includes("/operations/") ? Promise.resolve({
      ok: true, json: async () => ({ state: 42 }),
    }) : normal(url, options));
  fireEvent.click(screen.getByRole("button", { name: "Upload selected videos" }));
  if (mode === "missing-confirmation") {
    await screen.findByText("Replace existing video?", { selector: "h2" });
    fireEvent.click(screen.getByLabelText("I approve replacing this exact destination version"));
    fireEvent.submit(document.querySelector("[data-intake-dialog-form]"));
  }
  await screen.findByText(`Upload failed: ${mode === "missing-confirmation" ? "Invalid replacement confirmation" :
    mode === "creation-recovery" ? "Upload creation requires recovery." : "Invalid operation status"}`);
});

test.each([199, 300, 503])("chunk HTTP %s cannot finalize and preserves the server error or status", async status => {
  load();
  await select(new File(["abc"], "clip.mp4"));
  const Xhr = global.XMLHttpRequest;
  global.XMLHttpRequest = class extends Xhr {
    send() {
      this.status = status;
      this.responseText = JSON.stringify(status === 503 ? { error: "Storage offline" } : {});
      this.onload();
    }
  };
  fireEvent.click(screen.getByRole("button", { name: "Upload selected videos" }));
  await screen.findByText(`Upload failed: ${status === 503 ? "Storage offline" : `HTTP ${status}`}`);
  expect(fetch.mock.calls.filter(([url]) => url.endsWith("/finish"))).toHaveLength(0);
});

test("progress uses measured bytes only and includes the chunk offset", async () => {
  load();
  await select(new File([new Uint8Array(8 * 1024 * 1024 + 3)], "clip.mp4"));
  const Xhr = global.XMLHttpRequest;
  let active;
  global.XMLHttpRequest = class extends Xhr {
    send(data) {
      if (sent.length === 0) super.send(data);
      else active = this;
    }
  };
  fireEvent.click(screen.getByRole("button", { name: "Upload selected videos" }));
  await waitFor(() => expect(active).toBeDefined());
  const progress = screen.getByRole("progressbar");
  expect(progress).toHaveValue(8 * 1024 * 1024);
  active.upload.onprogress({ lengthComputable: false, loaded: 2 });
  expect(progress).toHaveValue(8 * 1024 * 1024);
  active.upload.onprogress({ lengthComputable: true, loaded: 2 });
  expect(progress).toHaveValue(8 * 1024 * 1024 + 2);
  active.status = 299;
  active.responseText = JSON.stringify({ received: 8 * 1024 * 1024 + 3 });
  active.onload();
  await screen.findByText(`Uploaded. SHA-256: ${"a".repeat(64)}`);
});

test("upload lifecycle distinguishes destination review, transfer, and queued verification", async () => {
  load();
  await select(new File(["abc"], "clip.mp4"));
  fireEvent.change(screen.getByLabelText("Choose local videos"), { target: { files: [new File(["bad"], "skip.txt")] } });
  const normal = fetch.getMockImplementation();
  let review, finish, verification, active;
  fetch.mockImplementation((url, options) => url.endsWith("/review") ? new Promise(resolve => { review = resolve; }) :
    url.endsWith("/finish") ? new Promise(resolve => { finish = resolve; }) :
      url.includes("/operations/") ? new Promise(resolve => { verification = resolve; }) : normal(url, options));
  const Xhr = global.XMLHttpRequest;
  global.XMLHttpRequest = class extends Xhr { send() { active = this; } };
  fireEvent.click(screen.getByRole("button", { name: "Upload selected videos" }));
  await screen.findByText("Reviewing destination...");
  const leaving = new Event("beforeunload", { cancelable: true });
  window.dispatchEvent(leaving);
  expect(leaving.defaultPrevented).toBe(true);
  const beforeUnload = window.addEventListener.mock.calls.find(([name]) => name === "beforeunload")[1];
  const browserEvent = { preventDefault: jest.fn(), returnValue: undefined };
  beforeUnload(browserEvent);
  expect(browserEvent.preventDefault).toHaveBeenCalledTimes(1);
  expect(browserEvent.returnValue).toBe("");
  review({ ok: true, json: async () => ({ exists: false }) });
  await screen.findByText("Transferring original video bytes...");
  await waitFor(() => expect(active).toBeDefined());
  active.status = 200; active.responseText = '{"received":3}'; active.onload();
  await waitFor(() => expect(finish).toBeDefined());
  finish({ ok: true, json: async () => ({ state: "queued" }) });
  await screen.findByText("Verifying complete ADLS bytes; transfer is not yet complete.");
  const verifying = new Event("beforeunload", { cancelable: true });
  window.dispatchEvent(verifying);
  expect(verifying.defaultPrevented).toBe(false);
  verification({ ok: true, json: async () => ({ state: "succeeded", sha256: "final" }) });
  await screen.findByText("Uploaded. SHA-256: final");
});

test.each(["cancel", "pagehide"])("%s between chunks prevents any additional video bytes", async kind => {
  load();
  await select(new File([new Uint8Array(8 * 1024 * 1024 + 3)], "clip.mp4"));
  const Xhr = global.XMLHttpRequest;
  global.XMLHttpRequest = class extends Xhr {
    send(data) {
      super.send(data);
      if (sent.length === 1) {
        if (kind === "cancel") fireEvent.click(screen.getByRole("button", { name: "Cancel upload" }));
        else window.dispatchEvent(new Event("pagehide"));
      }
    }
  };
  fireEvent.click(screen.getByRole("button", { name: "Upload selected videos" }));
  await screen.findByText("Upload failed: Upload cancelled");
  expect(sent).toHaveLength(1);
  expect(fetch.mock.calls.filter(([url]) => url.endsWith("/finish"))).toHaveLength(0);
});

test("move polling shows fallback and server progress before reporting completion", async () => {
  jest.useFakeTimers();
  load();
  checked("north/clip.mp4");
  const normal = fetch.getMockImplementation();
  let polls = 0;
  fetch.mockImplementation((url, options) => url.includes("/operations/") ? Promise.resolve({
    ok: true, json: async () => ++polls === 1 ? { state: "moving" } :
      polls === 2 ? { state: "moving", message: "Updating editable references" } : { state: "completed" },
  }) : normal(url, options));
  fireEvent.click(screen.getByRole("button", { name: "Move selected" }));
  fireEvent.submit(document.querySelector("[data-intake-dialog-form]"));
  await jest.advanceTimersByTimeAsync(0);
  await screen.findByText("Moving selected items...");
  await jest.advanceTimersByTimeAsync(1000);
  await screen.findByText("Updating editable references");
  await jest.advanceTimersByTimeAsync(1000);
  await screen.findByText("Move completed. Editable references updated; refresh the folder to view the new paths.");
  expect(fetch.mock.calls.find(([url]) => url.endsWith("/review"))[1].body.get("path")).toBe("clip.mp4");
});

test("download refuses a mixed video and directory selection", async () => {
  load();
  const open = jest.spyOn(window, "open").mockImplementation(() => null);
  checked("north/clip.mp4");
  checked("north/folder", true);
  fireEvent.click(screen.getByRole("button", { name: "Download selected" }));
  await screen.findByText("Storage action failed: Select supported videos for download.");
  expect(open).not.toHaveBeenCalled();
});
