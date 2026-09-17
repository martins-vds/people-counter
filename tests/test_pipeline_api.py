import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import torch

from people_counter.config import RFDetrBotsortConfig, RTDetrOsnetConfig
from people_counter.models import RunResult
from people_counter.pipelines.rfdetr_botsort import RFDetrRuntime
from people_counter.pipelines.rfdetr_botsort import run as run_botsort
from people_counter.pipelines.rtdetr_osnet import RTDetrRuntime
from people_counter.pipelines.rtdetr_osnet import run as run_rtdetr
from tests.helpers import FakeCapture, RecordingEmbedder


class FakeInputs(dict):
    def to(self, device):
        del device
        return self


class FakeProcessor:
    def __init__(self):
        self.call_count = 0

    def __call__(self, **kwargs):
        del kwargs
        return FakeInputs()

    def post_process_object_detection(self, outputs, target_sizes, threshold):
        del outputs, threshold
        x = self.call_count * 5
        self.call_count += 1
        return [
            {
                "boxes": torch.tensor(
                    [[x, 0, x + 20, 40]],
                    dtype=torch.float32,
                ),
                "labels": torch.tensor([0]),
                "scores": torch.tensor([0.9]),
            }
            for _ in target_sizes
        ]


class FakeRTDetrModel:
    def __init__(self, fail_on_call=None):
        self.call_count = 0
        self.fail_on_call = fail_on_call

    def __call__(self, **kwargs):
        del kwargs
        self.call_count += 1
        if self.call_count == self.fail_on_call:
            raise RuntimeError("detector failed")
        return object()


class FakeRFDetections:
    def __init__(self, class_names=None, tracker_ids=None):
        self.data = {
            "class_name": np.asarray(class_names or ["person"]),
        }
        self.tracker_id = (
            None
            if tracker_ids is None
            else np.asarray(tracker_ids, dtype=int)
        )

    def __getitem__(self, selector):
        selected_names = self.data["class_name"][selector].tolist()
        selected_ids = (
            None
            if self.tracker_id is None
            else self.tracker_id[selector].tolist()
        )
        return FakeRFDetections(selected_names, selected_ids)


class FakeRFDetrModel:
    def predict(self, frames, **kwargs):
        del kwargs
        return [FakeRFDetections() for _ in frames]


class FakeBoTSORTTracker:
    def update(self, detections, **kwargs):
        del detections, kwargs
        return FakeRFDetections(tracker_ids=[3])


class PipelineApiTests(unittest.TestCase):
    def test_rtdetr_rejects_reused_result_before_loading_models(self):
        config = RTDetrOsnetConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=1,
            result=RunResult(initialized=True),
        )

        with self.assertRaisesRegex(RuntimeError, "already populated"):
            run_rtdetr(config)

    def test_botsort_rejects_reused_result_before_loading_models(self):
        config = RFDetrBotsortConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=1,
            result=RunResult(initialized=True),
        )

        with self.assertRaisesRegex(RuntimeError, "already populated"):
            run_botsort(config)

    def test_rtdetr_run_processes_mocked_video_and_reports_progress(self):
        frames = [
            np.zeros((80, 120, 3), dtype=np.uint8),
            np.zeros((80, 120, 3), dtype=np.uint8),
        ]
        capture = FakeCapture(frames)
        progress = []
        runtime = RTDetrRuntime(
            device=torch.device("cpu"),
            reid_embedder=RecordingEmbedder(),
            processor=FakeProcessor(),
            model=FakeRTDetrModel(),
            person_class_id=0,
        )
        config = RTDetrOsnetConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=1,
            sample_fps=None,
            progress_callback=lambda result: progress.append(
                result.processed_frames
            ),
        )

        with (
            patch(
                "people_counter.pipelines.rtdetr_osnet.load_runtime",
                return_value=runtime,
            ),
            patch(
                "people_counter.pipelines.rtdetr_osnet.cv2.VideoCapture",
                return_value=capture,
            ),
        ):
            result = run_rtdetr(config)

        self.assertIs(result, config.result)
        self.assertTrue(capture.released)
        self.assertEqual(progress, [0, 1, 2])
        self.assertEqual(result.processed_frames, 2)
        self.assertEqual(result.source_frames_read, 2)
        self.assertFalse(result.ended_early)
        self.assertEqual(result.telemetry[1].entry_frame, 0)
        self.assertEqual(result.telemetry[1].last_seen_frame, 1)

    def test_rtdetr_run_retains_partial_result_after_detector_failure(self):
        frames = [
            np.zeros((80, 120, 3), dtype=np.uint8),
            np.zeros((80, 120, 3), dtype=np.uint8),
        ]
        capture = FakeCapture(frames)
        progress = []
        runtime = RTDetrRuntime(
            device=torch.device("cpu"),
            reid_embedder=RecordingEmbedder(),
            processor=FakeProcessor(),
            model=FakeRTDetrModel(fail_on_call=2),
            person_class_id=0,
        )
        config = RTDetrOsnetConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=1,
            sample_fps=None,
            progress_callback=lambda result: progress.append(
                result.processed_frames
            ),
        )

        with (
            patch(
                "people_counter.pipelines.rtdetr_osnet.load_runtime",
                return_value=runtime,
            ),
            patch(
                "people_counter.pipelines.rtdetr_osnet.cv2.VideoCapture",
                return_value=capture,
            ),
            self.assertRaisesRegex(RuntimeError, "detector failed"),
        ):
            run_rtdetr(config)

        self.assertTrue(capture.released)
        self.assertTrue(config.result.initialized)
        self.assertEqual(config.result.processed_frames, 1)
        self.assertEqual(config.result.source_frames_read, 2)
        self.assertEqual(progress, [0, 1])

    def test_botsort_run_processes_mocked_video_and_reports_progress(self):
        frames = [
            np.zeros((80, 120, 3), dtype=np.uint8),
            np.zeros((80, 120, 3), dtype=np.uint8),
        ]
        capture = FakeCapture(frames)
        progress = []
        config = RFDetrBotsortConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=2,
            sample_fps=None,
            progress_callback=lambda result: progress.append(
                result.processed_frames
            ),
        )

        with (
            patch(
                "people_counter.pipelines.rfdetr_botsort.load_runtime",
                return_value=RFDetrRuntime(model=FakeRFDetrModel()),
            ),
            patch(
                "people_counter.pipelines.rfdetr_botsort.BoTSORTTracker",
                return_value=FakeBoTSORTTracker(),
            ),
            patch(
                "people_counter.pipelines.rfdetr_botsort.cv2.VideoCapture",
                return_value=capture,
            ),
        ):
            result = run_botsort(config)

        self.assertIs(result, config.result)
        self.assertTrue(capture.released)
        self.assertEqual(progress, [0, 2])
        self.assertEqual(result.processed_frames, 2)
        self.assertEqual(result.source_frames_read, 2)
        self.assertFalse(result.ended_early)
        self.assertTrue(result.camera_motion_compensation)
        self.assertEqual(result.telemetry[3].entry_frame, 0)
        self.assertEqual(result.telemetry[3].last_seen_frame, 1)


if __name__ == "__main__":
    unittest.main()
