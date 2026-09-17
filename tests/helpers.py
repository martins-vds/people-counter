import numpy as np

from people_counter.models import Detection


def detection(
    x: int,
    confidence: float = 0.9,
    embedding: tuple[float, float] = (1.0, 0.0),
) -> Detection:
    return Detection(
        bbox=(x, 0, x + 20, 40),
        centroid=(x + 10, 20),
        confidence=confidence,
        embedding=np.asarray(embedding, dtype=np.float32),
    )


class FakeCapture:
    def __init__(self, frames):
        self.frames = list(frames)
        self.index = 0

    def isOpened(self):
        return True

    def read(self):
        if self.index == len(self.frames):
            return False, None
        frame = self.frames[self.index]
        self.index += 1
        return True, frame

    def grab(self):
        if self.index == len(self.frames):
            return False
        self.index += 1
        return True


class RecordingEmbedder:
    def __init__(self):
        self.boxes = None

    def __call__(self, frame, boxes):
        del frame
        self.boxes = boxes
        return np.tile(
            np.asarray([1.0, 0.0], dtype=np.float32),
            (len(boxes), 1),
        )
