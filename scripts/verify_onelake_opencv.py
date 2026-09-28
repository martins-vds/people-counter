"""Paste this cell into a Fabric scratchpad notebook to test direct OneLake access."""

from pathlib import Path

import cv2


ONELAKE_VIDEO_PATH = Path(
    "/lakehouse/default/Files/videos/incoming/2023/09/29/5b300380f4ea792bf57f1ee1ff6a015258b9da63f5ccdd34bdfbcfb8d3564468/d5bd760178a4ae0d429a52c2c46ae6ec567fea2d55608e982293bfc001a8f750/subway.mp4"
)

if not ONELAKE_VIDEO_PATH.is_file():
    raise FileNotFoundError(f"OneLake file is not mounted: {ONELAKE_VIDEO_PATH}")

capture = cv2.VideoCapture(str(ONELAKE_VIDEO_PATH))
try:
    if not capture.isOpened():
        raise RuntimeError(f"OpenCV could not open: {ONELAKE_VIDEO_PATH}")

    ok, frame = capture.read()
    if not ok or frame is None:
        raise RuntimeError(f"OpenCV opened but could not decode: {ONELAKE_VIDEO_PATH}")

    print(
        "Direct OneLake read succeeded:",
        ONELAKE_VIDEO_PATH,
        f"first_frame={frame.shape[1]}x{frame.shape[0]}",
        f"fps={capture.get(cv2.CAP_PROP_FPS):.2f}",
        f"frames={int(capture.get(cv2.CAP_PROP_FRAME_COUNT))}",
    )
finally:
    capture.release()
