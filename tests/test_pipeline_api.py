import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import torch

from people_counter.api import (
    load_runtime,
    run_with_runtime,
)
from people_counter.config import RFDetrBotsortConfig, RTDetrOsnetConfig
from people_counter.models import RunResult
from people_counter.pipelines.rfdetr_botsort import RFDetrRuntime
from people_counter.pipelines.rfdetr_botsort import run as run_botsort
from people_counter.pipelines.rfdetr_botsort import (
    run_with_runtime as run_botsort_with_runtime,
)
from people_counter.pipelines.rtdetr_osnet import RTDetrRuntime
from people_counter.pipelines.rtdetr_osnet import run as run_rtdetr
from people_counter.pipelines.rtdetr_osnet import (
    run_with_runtime as run_rtdetr_with_runtime,
)
from people_counter.video import read_video_metadata
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

        with (
            patch(
                "people_counter.pipelines.rtdetr_osnet.load_runtime",
            ) as load_models,
            self.assertRaises(RuntimeError) as raised,
        ):
            run_rtdetr(config)

        load_models.assert_not_called()
        self.assertEqual(
            str(raised.exception),
            "RunResult already populated; create a new config for each run",
        )

    def test_botsort_rejects_reused_result_before_loading_models(self):
        config = RFDetrBotsortConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=1,
            result=RunResult(initialized=True),
        )

        with (
            patch(
                "people_counter.pipelines.rfdetr_botsort.load_runtime",
            ) as load_models,
            self.assertRaises(RuntimeError) as raised,
        ):
            run_botsort(config)

        load_models.assert_not_called()
        self.assertEqual(
            str(raised.exception),
            "RunResult already populated; create a new config for each run",
        )

    def test_public_runtime_api_loads_and_reuses_typed_runtime(self):
        config = RTDetrOsnetConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=1,
        )
        runtime = RTDetrRuntime(
            device=torch.device("cpu"),
            reid_embedder=MagicMock(),
            processor=MagicMock(),
            model=MagicMock(),
            person_class_id=0,
        )
        result = RunResult(initialized=True)

        with (
            patch(
                "people_counter.pipelines.rtdetr_osnet.load_runtime",
                return_value=runtime,
            ) as load_pipeline_runtime,
            patch(
                "people_counter.pipelines.rtdetr_osnet.run_with_runtime",
                return_value=result,
            ) as run_pipeline,
        ):
            loaded = load_runtime(config)
            actual = run_with_runtime(config, loaded)

        self.assertIs(loaded, runtime)
        self.assertIs(actual, result)
        load_pipeline_runtime.assert_called_once_with(config)
        run_pipeline.assert_called_once_with(config, runtime)

    def test_public_runtime_api_rejects_pipeline_mismatch(self):
        config = RTDetrOsnetConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=1,
        )
        runtime = RFDetrRuntime(model=MagicMock())

        with self.assertRaises(TypeError) as raised:
            run_with_runtime(config, runtime)
        self.assertEqual(
            str(raised.exception),
            "RTDetrOsnetConfig requires an RTDetrRuntime",
        )

    def test_public_runtime_api_dispatches_rfdetr_runtime(self):
        config = RFDetrBotsortConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=1,
        )
        runtime = RFDetrRuntime(model=MagicMock())
        result = RunResult(initialized=True)

        with (
            patch(
                "people_counter.pipelines.rfdetr_botsort.load_runtime",
                return_value=runtime,
            ) as load_pipeline_runtime,
            patch(
                "people_counter.pipelines.rfdetr_botsort.run_with_runtime",
                return_value=result,
            ) as run_pipeline,
        ):
            loaded = load_runtime(config)
            actual = run_with_runtime(config, loaded)

        self.assertIs(loaded, runtime)
        self.assertIs(actual, result)
        load_pipeline_runtime.assert_called_once_with(config)
        run_pipeline.assert_called_once_with(config, runtime)

    def test_public_runtime_api_rejects_rfdetr_runtime_mismatch(self):
        config = RFDetrBotsortConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=1,
        )
        runtime = RTDetrRuntime(
            device=torch.device("cpu"),
            reid_embedder=MagicMock(),
            processor=MagicMock(),
            model=MagicMock(),
            person_class_id=0,
        )

        with self.assertRaises(TypeError) as raised:
            run_with_runtime(config, runtime)
        self.assertEqual(
            str(raised.exception),
            "RFDetrBotsortConfig requires an RFDetrRuntime",
        )

    def test_public_runtime_api_rejects_unknown_config(self):
        config = MagicMock()

        with self.assertRaises(TypeError) as load_error:
            load_runtime(config)
        with self.assertRaises(TypeError) as run_error:
            run_with_runtime(config, MagicMock())

        expected = (
            "config must be RTDetrOsnetConfig or RFDetrBotsortConfig; "
            "got MagicMock"
        )
        self.assertEqual(str(load_error.exception), expected)
        self.assertEqual(str(run_error.exception), expected)

    def test_rtdetr_run_loads_and_passes_runtime(self):
        config = RTDetrOsnetConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=1,
        )
        runtime = MagicMock(spec=RTDetrRuntime)
        result = RunResult(initialized=True)

        with (
            patch(
                "people_counter.pipelines.rtdetr_osnet.load_runtime",
                return_value=runtime,
            ) as load_models,
            patch(
                "people_counter.pipelines.rtdetr_osnet.run_with_runtime",
                return_value=result,
            ) as run_loaded,
        ):
            actual = run_rtdetr(config)

        self.assertIs(actual, result)
        load_models.assert_called_once_with(config)
        run_loaded.assert_called_once_with(config=config, runtime=runtime)

    def test_botsort_run_loads_and_passes_runtime(self):
        config = RFDetrBotsortConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=1,
        )
        runtime = MagicMock(spec=RFDetrRuntime)
        result = RunResult(initialized=True)

        with (
            patch(
                "people_counter.pipelines.rfdetr_botsort.load_runtime",
                return_value=runtime,
            ) as load_models,
            patch(
                "people_counter.pipelines.rfdetr_botsort.run_with_runtime",
                return_value=result,
            ) as run_loaded,
        ):
            actual = run_botsort(config)

        self.assertIs(actual, result)
        load_models.assert_called_once_with(config)
        run_loaded.assert_called_once_with(config=config, runtime=runtime)

    def test_rtdetr_run_processes_mocked_video_and_reports_progress(self):
        frames = [
            np.zeros((80, 120, 3), dtype=np.uint8),
            np.zeros((80, 120, 3), dtype=np.uint8),
            np.zeros((80, 120, 3), dtype=np.uint8),
        ]
        capture = FakeCapture(frames)
        progress = []
        line_zone = MagicMock(in_count=4, out_count=3)
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
            line=(60, 0, 60, 79),
            progress_callback=lambda result: progress.append(
                result.processed_frames
            ),
        )

        with (
            patch(
                "people_counter.pipelines.rtdetr_osnet.cv2.VideoCapture",
                return_value=capture,
            ) as open_capture,
            patch(
                "people_counter.pipelines.rtdetr_osnet.read_video_metadata",
                wraps=read_video_metadata,
            ) as read_metadata,
            patch(
                "people_counter.pipelines.rtdetr_osnet.create_line_zone",
                return_value=line_zone,
            ),
            patch(
                "people_counter.pipelines.rtdetr_osnet.record_line_counts",
            ),
        ):
            result = run_rtdetr_with_runtime(config, runtime)

        open_capture.assert_called_once_with("video.mp4")
        read_metadata.assert_called_once_with(capture, Path("video.mp4"))
        self.assertIs(result, config.result)
        self.assertTrue(capture.released)
        self.assertEqual(progress, [0, 1, 2, 3])
        self.assertEqual(result.processed_frames, 3)
        self.assertEqual(result.source_frames_read, 3)
        self.assertFalse(result.ended_early)
        self.assertEqual(result.line_in_count, 4)
        self.assertEqual(result.line_out_count, 3)
        self.assertEqual(result.telemetry[1].entry_frame, 0)
        self.assertEqual(result.telemetry[1].last_seen_frame, 2)

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
                "people_counter.pipelines.rtdetr_osnet.cv2.VideoCapture",
                return_value=capture,
            ) as open_capture,
            self.assertRaisesRegex(RuntimeError, "detector failed"),
        ):
            run_rtdetr_with_runtime(config, runtime)

        open_capture.assert_called_once_with("video.mp4")
        self.assertTrue(capture.released)
        self.assertTrue(config.result.initialized)
        self.assertEqual(config.result.processed_frames, 1)
        self.assertEqual(config.result.source_frames_read, 2)
        self.assertEqual(progress, [0, 1])

    def test_botsort_run_processes_mocked_video_and_reports_progress(self):
        frames = [
            np.zeros((80, 120, 3), dtype=np.uint8),
            np.zeros((80, 120, 3), dtype=np.uint8),
            np.zeros((80, 120, 3), dtype=np.uint8),
        ]
        capture = FakeCapture(frames)
        progress = []
        line_zone = MagicMock(in_count=4, out_count=3)
        config = RFDetrBotsortConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=2,
            sample_fps=None,
            line=(60, 0, 60, 79),
            progress_callback=lambda result: progress.append(
                result.processed_frames
            ),
        )
        runtime = RFDetrRuntime(model=FakeRFDetrModel())

        with (
            patch(
                "people_counter.pipelines.rfdetr_botsort.BoTSORTTracker",
                return_value=FakeBoTSORTTracker(),
            ),
            patch(
                "people_counter.pipelines.rfdetr_botsort.cv2.VideoCapture",
                return_value=capture,
            ) as open_capture,
            patch(
                "people_counter.pipelines.rfdetr_botsort.read_video_metadata",
                wraps=read_video_metadata,
            ) as read_metadata,
            patch(
                "people_counter.pipelines.rfdetr_botsort.create_line_zone",
                return_value=line_zone,
            ),
            patch(
                "people_counter.pipelines.rfdetr_botsort.record_line_counts",
            ),
            patch(
                "people_counter.pipelines.rfdetr_botsort.coasting_track_detections",
                return_value=object(),
            ),
        ):
            result = run_botsort_with_runtime(config, runtime)

        open_capture.assert_called_once_with("video.mp4")
        read_metadata.assert_called_once_with(capture, Path("video.mp4"))
        self.assertIs(result, config.result)
        self.assertTrue(capture.released)
        self.assertEqual(progress, [0, 2, 3])
        self.assertEqual(result.processed_frames, 3)
        self.assertEqual(result.source_frames_read, 3)
        self.assertFalse(result.ended_early)
        self.assertTrue(result.camera_motion_compensation)
        self.assertEqual(result.line_in_count, 4)
        self.assertEqual(result.line_out_count, 3)
        self.assertEqual(result.telemetry[3].entry_frame, 0)
        self.assertEqual(result.telemetry[3].last_seen_frame, 2)

    def test_rtdetr_run_rejects_unopenable_video(self):
        capture = FakeCapture([np.zeros((1, 1, 3), dtype=np.uint8)])
        capture.isOpened = lambda: False
        config = RTDetrOsnetConfig(
            video=Path("missing.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=1,
        )

        with (
            patch(
                "people_counter.pipelines.rtdetr_osnet.load_runtime",
                return_value=MagicMock(spec=RTDetrRuntime),
            ),
            patch(
                "people_counter.pipelines.rtdetr_osnet.cv2.VideoCapture",
                return_value=capture,
            ),
            self.assertRaisesRegex(
                RuntimeError,
                "^Could not open input video: missing.mp4$",
            ),
        ):
            run_rtdetr(config)

    def test_botsort_run_rejects_unopenable_video(self):
        capture = FakeCapture([np.zeros((1, 1, 3), dtype=np.uint8)])
        capture.isOpened = lambda: False
        config = RFDetrBotsortConfig(
            video=Path("missing.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=1,
        )

        with (
            patch(
                "people_counter.pipelines.rfdetr_botsort.load_runtime",
                return_value=MagicMock(spec=RFDetrRuntime),
            ),
            patch(
                "people_counter.pipelines.rfdetr_botsort.cv2.VideoCapture",
                return_value=capture,
            ),
            self.assertRaisesRegex(
                RuntimeError,
                "^Could not open input video: missing.mp4$",
            ),
        ):
            run_botsort(config)

    def test_botsort_run_preserves_progress_callback_failure(self):
        capture = FakeCapture(
            [np.zeros((80, 120, 3), dtype=np.uint8)]
        )
        progress_error = RuntimeError("progress failed")

        def fail_progress(_):
            raise progress_error

        config = RFDetrBotsortConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=1,
            progress_callback=fail_progress,
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
            self.assertRaises(RuntimeError) as raised,
        ):
            run_botsort(config)

        self.assertIs(raised.exception, progress_error)
        self.assertTrue(capture.released)


if __name__ == "__main__":
    unittest.main()
