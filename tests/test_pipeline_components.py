import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import supervision as sv
import torch

from people_counter.config import (
    RFDetrBotsortConfig,
    RTDetrOsnetConfig,
    SamplingConfig,
)
from people_counter.models import RunResult
from people_counter.model_artifacts import (
    OSNET_FILENAME,
    OSNET_MODEL_DIR,
    RFDETR_FILENAME,
    RFDETR_PIPELINE_DIR,
    RTDETR_MODEL_DIRS,
    RTDETR_PIPELINE_DIR,
    RTDETR_REQUIRED_FILES,
)
from people_counter.pipelines import rfdetr_botsort, rtdetr_osnet
from people_counter.video import FrameReadState, VideoMetadata
from tests.helpers import FakeCapture


class RecordingInputs(dict):
    def __init__(self):
        super().__init__()
        self.device = None

    def to(self, device):
        self.device = device
        return self


class RecordingContextManager:
    def __init__(self):
        self.entered = False
        self.exited = False

    def __enter__(self):
        self.entered = True
        return self

    def __exit__(self, *_):
        self.exited = True
        return False


class ArrayDetections:
    def __init__(self, class_names, tracker_ids=None):
        self.data = {"class_name": np.asarray(class_names)}
        self.tracker_id = (
            None
            if tracker_ids is None
            else np.asarray(tracker_ids, dtype=int)
        )

    def __getitem__(self, selector):
        return ArrayDetections(
            self.data["class_name"][selector].tolist(),
            (
                None
                if self.tracker_id is None
                else self.tracker_id[selector].tolist()
            ),
        )


class RuntimeLoadingTests(unittest.TestCase):
    def test_rtdetr_load_reid_embedder_verifies_weights(self):
        weights = b"known weights"
        with tempfile.TemporaryDirectory() as directory:
            weights_path = Path(directory, "weights.pt")
            weights_path.write_bytes(weights)
            embedder = object()

            with (
                patch.object(
                    rtdetr_osnet,
                    "hf_hub_download",
                    return_value=str(weights_path),
                ) as download,
                patch.object(
                    rtdetr_osnet,
                    "REID_SHA256",
                    hashlib.sha256(weights).hexdigest(),
                ),
                patch.object(
                    rtdetr_osnet,
                    "OSNetEmbedder",
                    return_value=embedder,
                ) as embedder_type,
            ):
                result = rtdetr_osnet.load_reid_embedder(
                    torch.device("cpu")
                )

        self.assertIs(result, embedder)
        download.assert_called_once_with(
            repo_id=rtdetr_osnet.REID_REPO_ID,
            filename=rtdetr_osnet.REID_FILENAME,
            revision=rtdetr_osnet.REID_REVISION,
        )
        embedder_type.assert_called_once_with(
            variant="osnet_ain_x0_25",
            weights=weights_path,
            device="cpu",
        )

    def test_rtdetr_load_reid_embedder_rejects_checksum_mismatch(self):
        with tempfile.TemporaryDirectory() as directory:
            weights_path = Path(directory, "weights.pt")
            weights_path.write_bytes(b"unexpected weights")
            with (
                patch.object(
                    rtdetr_osnet,
                    "hf_hub_download",
                    return_value=str(weights_path),
                ),
                self.assertRaisesRegex(
                    RuntimeError,
                    "ReID weights checksum mismatch",
                ),
            ):
                rtdetr_osnet.load_reid_embedder(torch.device("cpu"))

    def test_rtdetr_load_reid_embedder_uses_local_weights_without_hub(self):
        weights = b"offline weights"
        with tempfile.TemporaryDirectory() as directory:
            weights_path = Path(directory, "weights.pt")
            weights_path.write_bytes(weights)
            embedder = object()
            with (
                patch.object(
                    rtdetr_osnet,
                    "REID_SHA256",
                    hashlib.sha256(weights).hexdigest(),
                ),
                patch.object(
                    rtdetr_osnet,
                    "hf_hub_download",
                ) as download,
                patch.object(
                    rtdetr_osnet,
                    "OSNetEmbedder",
                    return_value=embedder,
                ),
            ):
                result = rtdetr_osnet.load_reid_embedder(
                    torch.device("cpu"),
                    weights_path,
                )

        self.assertIs(result, embedder)
        download.assert_not_called()

    def test_rtdetr_resolves_exactly_one_person_class(self):
        self.assertEqual(
            rtdetr_osnet.resolve_person_class_id(
                {0: "car", "7": " Person "}
            ),
            7,
        )
        for labels in ({0: "car"}, {0: "person", 1: "PERSON"}):
            with self.subTest(labels=labels):
                with self.assertRaises(RuntimeError) as raised:
                    rtdetr_osnet.resolve_person_class_id(labels)
                self.assertEqual(
                    str(raised.exception),
                    "Detector label map must contain exactly one 'person' class",
                )

    def test_rtdetr_load_runtime_wires_selected_model_and_gpu(self):
        config = RTDetrOsnetConfig(
            video=Path("video.mp4"),
            device_variant="gpu",
            device="cuda:0",
            batch_size=2,
            detector_model="r50",
        )
        embedder = object()
        processor = object()
        model = MagicMock()
        model.to.return_value = model
        model.config.id2label = {3: "person"}
        original_benchmark = torch.backends.cudnn.benchmark

        try:
            torch.backends.cudnn.benchmark = False
            with (
                patch.object(
                    rtdetr_osnet,
                    "load_reid_embedder",
                    return_value=embedder,
                ) as load_embedder,
                patch.object(
                    rtdetr_osnet.AutoImageProcessor,
                    "from_pretrained",
                    return_value=processor,
                ) as load_processor,
                patch.object(
                    rtdetr_osnet.RTDetrV2ForObjectDetection,
                    "from_pretrained",
                    return_value=model,
                ) as load_model,
            ):
                runtime = rtdetr_osnet.load_runtime(config)
                self.assertTrue(torch.backends.cudnn.benchmark)
        finally:
            torch.backends.cudnn.benchmark = original_benchmark

        expected_model = rtdetr_osnet.DETECTOR_MODELS["r50"]
        self.assertEqual(runtime.device, torch.device("cuda:0"))
        self.assertIs(runtime.reid_embedder, embedder)
        self.assertIs(runtime.processor, processor)
        self.assertIs(runtime.model, model)
        self.assertEqual(runtime.person_class_id, 3)
        load_embedder.assert_called_once_with(torch.device("cuda:0"))
        load_processor.assert_called_once_with(expected_model)
        load_model.assert_called_once_with(expected_model)
        model.to.assert_called_once_with(torch.device("cuda:0"))

    def test_rtdetr_load_runtime_uses_offline_artifacts_only(self):
        with tempfile.TemporaryDirectory() as directory:
            models_dir = Path(directory)
            detector_dir = (
                models_dir
                / RTDETR_PIPELINE_DIR
                / RTDETR_MODEL_DIRS["r50"]
            )
            detector_dir.mkdir(parents=True)
            for filename in RTDETR_REQUIRED_FILES:
                (detector_dir / filename).touch()
            reid_path = (
                models_dir
                / RTDETR_PIPELINE_DIR
                / OSNET_MODEL_DIR
                / OSNET_FILENAME
            )
            reid_path.parent.mkdir()
            reid_path.touch()
            config = RTDetrOsnetConfig(
                video=Path("video.mp4"),
                device_variant="cpu",
                device="cpu",
                batch_size=1,
                detector_model="r50",
                models_dir=models_dir,
            )
            processor = object()
            embedder = object()
            model = MagicMock()
            model.to.return_value = model
            model.config.id2label = {1: "person"}

            with (
                patch.object(
                    rtdetr_osnet,
                    "load_reid_embedder",
                    return_value=embedder,
                ) as load_embedder,
                patch.object(
                    rtdetr_osnet.AutoImageProcessor,
                    "from_pretrained",
                    return_value=processor,
                ) as load_processor,
                patch.object(
                    rtdetr_osnet.RTDetrV2ForObjectDetection,
                    "from_pretrained",
                    return_value=model,
                ) as load_model,
            ):
                runtime = rtdetr_osnet.load_runtime(config)

        self.assertIs(runtime.reid_embedder, embedder)
        load_embedder.assert_called_once_with(torch.device("cpu"), reid_path)
        load_processor.assert_called_once_with(
            detector_dir,
            local_files_only=True,
        )
        load_model.assert_called_once_with(
            detector_dir,
            local_files_only=True,
        )

    def test_botsort_load_runtime_configures_inference(self):
        model = MagicMock()
        with patch.object(
            rfdetr_botsort,
            "RFDETRLarge",
            return_value=model,
        ) as model_type:
            runtime = rfdetr_botsort.load_runtime(
                RFDetrBotsortConfig(
                    video=Path("video.mp4"),
                    device_variant="cpu",
                    device="cpu",
                    batch_size=4,
                    use_fp16=True,
                )
            )

        self.assertIs(runtime.model, model)
        model_type.assert_called_once_with(device="cpu")
        model.inference.assert_called_once_with(
            compile=False,
            batch_size=4,
            dtype=torch.float16,
            inplace=True,
        )

    def test_botsort_load_runtime_uses_float32_by_default(self):
        model = MagicMock()
        with patch.object(
            rfdetr_botsort,
            "RFDETRLarge",
            return_value=model,
        ):
            rfdetr_botsort.load_runtime(
                RFDetrBotsortConfig(
                    video=Path("video.mp4"),
                    device_variant="cpu",
                    device="cpu",
                    batch_size=2,
                )
            )

        self.assertEqual(
            model.inference.call_args.kwargs["dtype"],
            torch.float32,
        )

    def test_botsort_load_runtime_uses_offline_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            models_dir = Path(directory)
            checkpoint = (
                models_dir
                / RFDETR_PIPELINE_DIR
                / RFDETR_FILENAME
            )
            checkpoint.parent.mkdir()
            checkpoint.touch()
            model = MagicMock()
            with patch.object(
                rfdetr_botsort,
                "RFDETRLarge",
                return_value=model,
            ) as model_type:
                runtime = rfdetr_botsort.load_runtime(
                    RFDetrBotsortConfig(
                        video=Path("video.mp4"),
                        device_variant="cpu",
                        device="cpu",
                        batch_size=1,
                        models_dir=models_dir,
                    )
                )

        self.assertIs(runtime.model, model)
        model_type.assert_called_once_with(
            device="cpu",
            pretrain_weights=str(checkpoint),
        )


class RunStateTests(unittest.TestCase):
    def test_rtdetr_initialization_populates_complete_state_and_result(self):
        progress = []
        config = RTDetrOsnetConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=4,
            sample_fps=3.0,
            detection_threshold=0.65,
            use_fp16=True,
            line=(1, 2, 3, 4),
            detector_model="r50",
            progress_callback=lambda result: progress.append(
                (
                    result.initialized,
                    result.fps,
                    result.total_source_frames,
                    result.total_sampled_frames,
                    result.sample_interval,
                    result.effective_sample_fps,
                    result.batch_size,
                    result.use_fp16,
                )
            ),
        )
        runtime = MagicMock(spec=rtdetr_osnet.RTDetrRuntime)
        tracking = rtdetr_osnet.RTDetrTrackingState()
        metadata = VideoMetadata(
            fps=30.0,
            width=120,
            height=80,
            total_source_frames=61,
        )
        line_zone = object()

        with patch.object(
            rtdetr_osnet,
            "create_line_zone",
            return_value=line_zone,
        ) as create_zone:
            state = rtdetr_osnet.initialize_run_state(
                config,
                runtime,
                tracking,
                metadata,
            )

        create_zone.assert_called_once_with((1, 2, 3, 4), 120, 80)
        self.assertIs(state.config, config)
        self.assertIs(state.runtime, runtime)
        self.assertIs(state.tracking, tracking)
        self.assertIs(state.metadata, metadata)
        self.assertEqual(state.sampling.interval, 10)
        self.assertEqual(state.sampling.effective_fps, 3.0)
        self.assertEqual(state.sampling.total_sampled_frames, 7)
        self.assertEqual(state.max_disappeared_frames, 3)
        self.assertEqual(state.max_reentry_frames, 900)
        self.assertEqual(state.max_centroid_displacement, 20.0)
        self.assertIs(state.line_zone, line_zone)
        self.assertEqual(
            progress,
            [(True, 30.0, 61, 7, 10, 3.0, 4, True)],
        )

    def test_botsort_initialization_configures_tracker_and_result(self):
        progress = []
        config = RFDetrBotsortConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=4,
            sample_fps=3.0,
            detection_threshold=0.65,
            use_fp16=True,
            line=(1, 2, 3, 4),
            progress_callback=lambda result: progress.append(
                (
                    result.initialized,
                    result.fps,
                    result.total_source_frames,
                    result.total_sampled_frames,
                    result.sample_interval,
                    result.effective_sample_fps,
                    result.batch_size,
                    result.use_fp16,
                    result.camera_motion_compensation,
                )
            ),
        )
        runtime = MagicMock(spec=rfdetr_botsort.RFDetrRuntime)
        metadata = VideoMetadata(
            fps=30.0,
            width=120,
            height=80,
            total_source_frames=61,
        )
        line_zone = object()
        tracker = object()

        with (
            patch.object(
                rfdetr_botsort,
                "create_line_zone",
                return_value=line_zone,
            ) as create_zone,
            patch.object(
                rfdetr_botsort,
                "BoTSORTTracker",
                return_value=tracker,
            ) as tracker_type,
        ):
            state = rfdetr_botsort.initialize_run_state(
                config,
                runtime,
                metadata,
            )

        create_zone.assert_called_once_with((1, 2, 3, 4), 120, 80)
        tracker_type.assert_called_once_with(
            frame_rate=30.0,
            lost_track_buffer=30,
            track_activation_threshold=0.65,
            high_conf_det_threshold=0.65,
            minimum_consecutive_frames=2,
            instant_first_frame_activation=False,
            enable_cmc=False,
        )
        self.assertIs(state.config, config)
        self.assertIs(state.runtime, runtime)
        self.assertIs(state.metadata, metadata)
        self.assertEqual(state.sampling.interval, 10)
        self.assertEqual(state.sampling.effective_fps, 3.0)
        self.assertEqual(state.sampling.total_sampled_frames, 7)
        self.assertIs(state.tracker, tracker)
        self.assertEqual(state.lost_track_buffer, 30)
        self.assertEqual(state.line_track_cache, {})
        self.assertIs(state.line_zone, line_zone)
        self.assertEqual(
            progress,
            [(True, 30.0, 61, 7, 10, 3.0, 4, True, False)],
        )

    def test_botsort_initialization_honors_explicit_cmc(self):
        config = RFDetrBotsortConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=1,
            sample_fps=3.0,
            camera_motion_compensation=True,
        )
        with patch.object(
            rfdetr_botsort,
            "BoTSORTTracker",
        ) as tracker_type:
            state = rfdetr_botsort.initialize_run_state(
                config,
                MagicMock(spec=rfdetr_botsort.RFDetrRuntime),
                VideoMetadata(30.0, 120, 80, 10),
            )

        self.assertTrue(state.config.result.camera_motion_compensation)
        self.assertTrue(tracker_type.call_args.kwargs["enable_cmc"])


class FinalizationTests(unittest.TestCase):
    def test_rtdetr_finalization_records_partial_run_and_line_totals(self):
        self._assert_finalization(rtdetr_osnet)

    def test_botsort_finalization_records_partial_run_and_line_totals(self):
        self._assert_finalization(rfdetr_botsort)

    def test_botsort_finalization_ignores_line_totals_before_initialization(
        self,
    ):
        result = RunResult()
        capture = FakeCapture([np.zeros((1, 1, 3), dtype=np.uint8)])
        line_zone = MagicMock(in_count=3, out_count=2)

        rfdetr_botsort.finalize_run(
            result,
            capture,
            FrameReadState(),
            processing_started=0.0,
            line_zone=line_zone,
        )

        self.assertEqual(result.line_in_count, 0)
        self.assertEqual(result.line_out_count, 0)

    def _assert_finalization(self, pipeline):
        result = RunResult(initialized=True)
        capture = FakeCapture([np.zeros((1, 1, 3), dtype=np.uint8)])
        read_state = FrameReadState(
            source_frames_read=8,
            ended_early=True,
        )
        line_zone = MagicMock(in_count=3, out_count=2)

        with patch.object(
            pipeline.time,
            "perf_counter",
            return_value=15.5,
        ):
            pipeline.finalize_run(
                result,
                capture,
                read_state,
                processing_started=10.0,
                line_zone=line_zone,
            )

        self.assertTrue(capture.released)
        self.assertEqual(result.processing_seconds, 5.5)
        self.assertEqual(result.source_frames_read, 8)
        self.assertTrue(result.ended_early)
        self.assertEqual(result.line_in_count, 3)
        self.assertEqual(result.line_out_count, 2)


class RunExecutionTests(unittest.TestCase):
    def test_rtdetr_run_preserves_partial_initialization_failure_state(self):
        progress = []
        config = RTDetrOsnetConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=1,
            result=RunResult(),
            progress_callback=lambda result: progress.append(
                (
                    result.initialized,
                    result.fps,
                    result.processing_seconds,
                )
            ),
        )
        runtime = MagicMock(spec=rtdetr_osnet.RTDetrRuntime)
        metadata = VideoMetadata(
            fps=30.0,
            width=120,
            height=80,
            total_source_frames=10,
        )
        capture = FakeCapture([np.zeros((2, 2, 3), dtype=np.uint8)])

        def fail_initialize(
            failing_config,
            runtime_arg,
            tracking_arg,
            metadata_arg,
        ):
            self.assertIs(failing_config, config)
            self.assertIs(runtime_arg, runtime)
            self.assertIs(metadata_arg, metadata)
            self.assertIs(tracking_arg.telemetry, config.result.telemetry)
            failing_config.result.fps = metadata_arg.fps
            failing_config.result.initialized = True
            rtdetr_osnet._notify_progress(failing_config)
            raise RuntimeError("RT-DETR initialization failed")

        with (
            patch.object(
                rtdetr_osnet,
                "load_runtime",
                return_value=runtime,
            ),
            patch.object(
                rtdetr_osnet.cv2,
                "VideoCapture",
                return_value=capture,
            ),
            patch.object(
                rtdetr_osnet,
                "read_video_metadata",
                return_value=metadata,
            ),
            patch.object(
                rtdetr_osnet,
                "initialize_run_state",
                side_effect=fail_initialize,
            ),
            patch.object(
                rtdetr_osnet.time,
                "perf_counter",
                side_effect=[10.0, 12.5],
            ),
            self.assertRaisesRegex(
                RuntimeError,
                "^RT-DETR initialization failed$",
            ),
        ):
            rtdetr_osnet.run(config)

        self.assertTrue(capture.released)
        self.assertEqual(
            progress,
            [(True, 30.0, 0.0)],
        )
        self.assertTrue(config.result.started)
        self.assertTrue(config.result.initialized)
        self.assertEqual(config.result.fps, 30.0)
        self.assertEqual(config.result.processing_seconds, 2.5)
        self.assertEqual(config.result.source_frames_read, 0)
        self.assertFalse(config.result.ended_early)
        self.assertEqual(config.result.line_in_count, 0)
        self.assertEqual(config.result.line_out_count, 0)


class FrameProcessingTests(unittest.TestCase):
    def test_rtdetr_predict_detector_results_preserves_inference_contract(
        self,
    ):
        inputs = RecordingInputs()
        processor = MagicMock(return_value=inputs)
        processed_results = object()
        processor.post_process_object_detection.return_value = (
            processed_results
        )
        outputs = object()
        model = MagicMock(return_value=outputs)
        config = RTDetrOsnetConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=2,
            use_fp16=False,
        )
        runtime = rtdetr_osnet.RTDetrRuntime(
            device=torch.device("cpu"),
            reid_embedder=object(),
            processor=processor,
            model=model,
            person_class_id=0,
        )
        state = rtdetr_osnet.RTDetrRunState(
            config=config,
            runtime=runtime,
            tracking=rtdetr_osnet.RTDetrTrackingState(),
            metadata=VideoMetadata(30.0, 120, 80, 61),
            sampling=SamplingConfig(1, 30.0, 61),
            max_disappeared_frames=30,
            max_reentry_frames=900,
            max_centroid_displacement=2.0,
            line_zone=None,
        )
        frame_a = np.zeros((80, 120, 3), dtype=np.uint8)
        frame_b = np.zeros((60, 100, 3), dtype=np.uint8)
        rgb_a = np.ones((80, 120, 3), dtype=np.uint8)
        rgb_b = np.ones((60, 100, 3), dtype=np.uint8)
        resized_a = object()
        resized_b = object()
        inference_mode = RecordingContextManager()
        autocast_mode = RecordingContextManager()
        real_tensor = torch.tensor

        with (
            patch.object(
                rtdetr_osnet.cv2,
                "resize",
                side_effect=[resized_a, resized_b],
            ) as resize,
            patch.object(
                rtdetr_osnet.torch,
                "inference_mode",
                return_value=inference_mode,
            ) as inference_factory,
            patch.object(
                rtdetr_osnet.torch,
                "autocast",
                return_value=autocast_mode,
            ) as autocast_factory,
            patch.object(
                rtdetr_osnet.torch,
                "tensor",
                side_effect=real_tensor,
            ) as tensor_factory,
        ):
            results = rtdetr_osnet.predict_detector_results(
                state,
                [frame_a, frame_b],
                [rgb_a, rgb_b],
            )

        self.assertIs(results, processed_results)
        self.assertEqual(
            resize.call_args_list,
            [
                unittest.mock.call(
                    rgb_a,
                    (640, 640),
                    interpolation=rtdetr_osnet.cv2.INTER_LINEAR,
                ),
                unittest.mock.call(
                    rgb_b,
                    (640, 640),
                    interpolation=rtdetr_osnet.cv2.INTER_LINEAR,
                ),
            ],
        )
        processor.assert_called_once_with(
            images=[resized_a, resized_b],
            do_resize=False,
            return_tensors="pt",
        )
        self.assertEqual(inputs.device, torch.device("cpu"))
        inference_factory.assert_called_once_with()
        self.assertTrue(inference_mode.entered)
        self.assertTrue(inference_mode.exited)
        autocast_factory.assert_called_once_with(
            device_type="cpu",
            dtype=torch.float16,
            enabled=False,
        )
        self.assertTrue(autocast_mode.entered)
        self.assertTrue(autocast_mode.exited)
        model.assert_called_once_with()
        self.assertEqual(
            tensor_factory.call_args.kwargs["device"],
            torch.device("cpu"),
        )
        post_process_call = (
            processor.post_process_object_detection.call_args
        )
        self.assertEqual(post_process_call.args, (outputs,))
        self.assertEqual(post_process_call.kwargs["threshold"], 0.1)
        self.assertTrue(
            torch.equal(
                post_process_call.kwargs["target_sizes"],
                torch.tensor([[80, 120], [60, 100]]),
            )
        )

    def test_rtdetr_process_frame_wires_detection_tracking_and_line_counting(
        self,
    ):
        config = RTDetrOsnetConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=2,
            detection_threshold=0.65,
            line=(1, 2, 3, 4),
        )
        runtime = rtdetr_osnet.RTDetrRuntime(
            device=torch.device("cpu"),
            reid_embedder=object(),
            processor=object(),
            model=object(),
            person_class_id=7,
        )
        tracking = rtdetr_osnet.RTDetrTrackingState()
        line_zone = object()
        state = rtdetr_osnet.RTDetrRunState(
            config=config,
            runtime=runtime,
            tracking=tracking,
            metadata=VideoMetadata(30.0, 120, 80, 61),
            sampling=SamplingConfig(10, 3.0, 7),
            max_disappeared_frames=3,
            max_reentry_frames=900,
            max_centroid_displacement=20.0,
            line_zone=line_zone,
        )
        frame = np.zeros((80, 120, 3), dtype=np.uint8)
        rgb_frame = np.ones((80, 120, 3), dtype=np.uint8)
        detector_result = object()
        detections = []
        line_detections = object()

        with (
            patch.object(
                rtdetr_osnet,
                "extract_person_detections",
                return_value=detections,
            ) as extract,
            patch.object(
                rtdetr_osnet,
                "associate_detections",
            ) as associate,
            patch.object(
                rtdetr_osnet,
                "active_track_detections",
                return_value=line_detections,
            ) as active_tracks,
            patch.object(
                rtdetr_osnet,
                "record_line_counts",
            ) as record_counts,
        ):
            rtdetr_osnet.process_frame(
                state,
                20,
                frame,
                rgb_frame,
                detector_result,
            )

        extract.assert_called_once_with(
            detector_result,
            frame,
            rgb_frame,
            runtime.reid_embedder,
            7,
            tracking.tracks,
            0.65,
        )
        associate.assert_called_once_with(
            detections,
            tracking,
            20,
            20.0,
            3,
            900,
            10,
            0.65,
        )
        active_tracks.assert_called_once_with(tracking.tracks)
        record_counts.assert_called_once_with(
            line_zone,
            line_detections,
            20,
            30.0,
            (1, 2, 3, 4),
            config.result.line_counts,
        )

    def test_rtdetr_process_batch_preserves_frame_correspondence(self):
        progress = []
        config = RTDetrOsnetConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=2,
            progress_callback=lambda result: progress.append(
                result.processed_frames
            ),
        )
        state = rtdetr_osnet.RTDetrRunState(
            config=config,
            runtime=MagicMock(spec=rtdetr_osnet.RTDetrRuntime),
            tracking=rtdetr_osnet.RTDetrTrackingState(),
            metadata=VideoMetadata(30.0, 120, 80, 61),
            sampling=SamplingConfig(10, 3.0, 7),
            max_disappeared_frames=3,
            max_reentry_frames=900,
            max_centroid_displacement=20.0,
            line_zone=None,
        )
        frame_a = np.zeros((2, 2, 3), dtype=np.uint8)
        frame_b = np.ones((2, 2, 3), dtype=np.uint8)
        rgb_a = object()
        rgb_b = object()
        result_a = object()
        result_b = object()

        with (
            patch.object(
                rtdetr_osnet.cv2,
                "cvtColor",
                side_effect=[rgb_a, rgb_b],
            ) as convert,
            patch.object(
                rtdetr_osnet,
                "predict_detector_results",
                return_value=[result_a, result_b],
            ) as predict,
            patch.object(rtdetr_osnet, "process_frame") as process,
        ):
            rtdetr_osnet.process_frame_batch(
                state,
                [(10, frame_a), (20, frame_b)],
            )

        self.assertEqual(
            convert.call_args_list,
            [
                unittest.mock.call(frame_a, rtdetr_osnet.cv2.COLOR_BGR2RGB),
                unittest.mock.call(frame_b, rtdetr_osnet.cv2.COLOR_BGR2RGB),
            ],
        )
        predict.assert_called_once_with(
            state,
            [frame_a, frame_b],
            [rgb_a, rgb_b],
        )
        self.assertEqual(
            process.call_args_list,
            [
                unittest.mock.call(state, 10, frame_a, rgb_a, result_a),
                unittest.mock.call(state, 20, frame_b, rgb_b, result_b),
            ],
        )
        self.assertEqual(config.result.processed_frames, 2)
        self.assertEqual(progress, [2])

    def test_botsort_process_frame_wires_tracking_and_line_counting(self):
        config = RFDetrBotsortConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=2,
            line=(1, 2, 3, 4),
        )
        line_zone = object()
        state = rfdetr_botsort.RFDetrRunState(
            config=config,
            runtime=MagicMock(spec=rfdetr_botsort.RFDetrRuntime),
            metadata=VideoMetadata(30.0, 120, 80, 61),
            sampling=SamplingConfig(10, 3.0, 7),
            tracker=MagicMock(),
            lost_track_buffer=30,
            line_track_cache={},
            line_zone=line_zone,
        )
        frame = np.zeros((80, 120, 3), dtype=np.uint8)
        detections = object()
        confirmed = object()
        coasted = object()

        with (
            patch.object(
                rfdetr_botsort,
                "_confirmed_people",
                return_value=confirmed,
            ) as confirm,
            patch.object(
                rfdetr_botsort,
                "coasting_track_detections",
                return_value=coasted,
            ) as coast,
            patch.object(
                rfdetr_botsort,
                "record_line_counts",
            ) as record_counts,
            patch.object(
                rfdetr_botsort,
                "_record_telemetry",
            ) as record_telemetry,
        ):
            rfdetr_botsort.process_frame(
                state,
                20,
                frame,
                detections,
            )

        confirm.assert_called_once_with(state, detections, frame, 20)
        coast.assert_called_once_with(
            confirmed,
            state.line_track_cache,
            20,
            30,
        )
        record_counts.assert_called_once_with(
            line_zone,
            coasted,
            20,
            30.0,
            (1, 2, 3, 4),
            config.result.line_counts,
        )
        record_telemetry.assert_called_once_with(state, confirmed, 20)

    def test_botsort_confirms_people_with_source_timestamp(self):
        tracker = MagicMock()
        tracker.update.return_value = ArrayDetections(
            ["person", "person"],
            [0, -1],
        )
        state = rfdetr_botsort.RFDetrRunState(
            config=RFDetrBotsortConfig(
                video=Path("video.mp4"),
                device_variant="cpu",
                device="cpu",
                batch_size=1,
            ),
            runtime=MagicMock(spec=rfdetr_botsort.RFDetrRuntime),
            metadata=VideoMetadata(20.0, 120, 80, 61),
            sampling=SamplingConfig(1, 20.0, 61),
            tracker=tracker,
            lost_track_buffer=30,
            line_track_cache={},
            line_zone=None,
        )
        frame = np.zeros((80, 120, 3), dtype=np.uint8)
        detections = ArrayDetections(["car", "person"], [5, 6])

        confirmed = rfdetr_botsort._confirmed_people(
            state,
            detections,
            frame,
            10,
        )

        tracked_input = tracker.update.call_args.args[0]
        self.assertEqual(tracked_input.data["class_name"].tolist(), ["person"])
        tracker.update.assert_called_once_with(
            tracked_input,
            frame=frame,
            timestamp=0.5,
        )
        self.assertEqual(confirmed.tracker_id.tolist(), [0])

    def test_botsort_confirmation_requires_class_names(self):
        detections = MagicMock()
        detections.data = {}
        state = rfdetr_botsort.RFDetrRunState(
            config=RFDetrBotsortConfig(
                video=Path("video.mp4"),
                device_variant="cpu",
                device="cpu",
                batch_size=1,
            ),
            runtime=MagicMock(spec=rfdetr_botsort.RFDetrRuntime),
            metadata=VideoMetadata(20.0, 120, 80, 61),
            sampling=SamplingConfig(1, 20.0, 61),
            tracker=MagicMock(),
            lost_track_buffer=30,
            line_track_cache={},
            line_zone=None,
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "^RF-DETR detections did not include class names$",
        ):
            rfdetr_botsort._confirmed_people(
                state,
                detections,
                np.zeros((1, 1, 3), dtype=np.uint8),
                0,
            )

    def test_botsort_confirmation_returns_empty_without_tracker_ids(self):
        tracker = MagicMock()
        tracker.update.return_value = ArrayDetections(["person"])
        state = rfdetr_botsort.RFDetrRunState(
            config=RFDetrBotsortConfig(
                video=Path("video.mp4"),
                device_variant="cpu",
                device="cpu",
                batch_size=1,
            ),
            runtime=MagicMock(spec=rfdetr_botsort.RFDetrRuntime),
            metadata=VideoMetadata(20.0, 120, 80, 61),
            sampling=SamplingConfig(1, 20.0, 61),
            tracker=tracker,
            lost_track_buffer=30,
            line_track_cache={},
            line_zone=None,
        )

        confirmed = rfdetr_botsort._confirmed_people(
            state,
            ArrayDetections(["person"]),
            np.zeros((1, 1, 3), dtype=np.uint8),
            0,
        )

        self.assertIsInstance(confirmed, sv.Detections)
        self.assertEqual(len(confirmed), 0)

    def test_botsort_telemetry_records_first_and_latest_seen_frames(self):
        config = RFDetrBotsortConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=1,
        )
        state = rfdetr_botsort.RFDetrRunState(
            config=config,
            runtime=MagicMock(spec=rfdetr_botsort.RFDetrRuntime),
            metadata=VideoMetadata(30.0, 120, 80, 61),
            sampling=SamplingConfig(10, 3.0, 7),
            tracker=MagicMock(),
            lost_track_buffer=30,
            line_track_cache={},
            line_zone=None,
        )
        confirmed = sv.Detections(
            xyxy=np.asarray([[0, 0, 20, 40]], dtype=np.float32),
            tracker_id=np.asarray([7], dtype=np.int32),
        )

        rfdetr_botsort._record_telemetry(state, confirmed, 20)

        self.assertEqual(config.result.telemetry[7].entry_frame, 10)
        self.assertEqual(config.result.telemetry[7].last_seen_frame, 20)

        rfdetr_botsort._record_telemetry(state, confirmed, 30)

        self.assertEqual(config.result.telemetry[7].entry_frame, 10)
        self.assertEqual(config.result.telemetry[7].last_seen_frame, 30)

    def test_botsort_process_batch_preserves_frame_correspondence(self):
        progress = []
        model = MagicMock()
        detection_a = object()
        detection_b = object()
        model.predict.return_value = [detection_a, detection_b]
        config = RFDetrBotsortConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=2,
            progress_callback=lambda result: progress.append(
                result.processed_frames
            ),
        )
        state = rfdetr_botsort.RFDetrRunState(
            config=config,
            runtime=rfdetr_botsort.RFDetrRuntime(model=model),
            metadata=VideoMetadata(30.0, 120, 80, 61),
            sampling=SamplingConfig(10, 3.0, 7),
            tracker=MagicMock(),
            lost_track_buffer=30,
            line_track_cache={},
            line_zone=None,
        )
        config.result.processed_frames = 5
        frame_a = np.zeros((2, 2, 3), dtype=np.uint8)
        frame_b = np.ones((2, 2, 3), dtype=np.uint8)
        rgb_a = object()
        rgb_b = object()

        with (
            patch.object(
                rfdetr_botsort.cv2,
                "cvtColor",
                side_effect=[rgb_a, rgb_b],
            ) as convert,
            patch.object(rfdetr_botsort, "process_frame") as process,
        ):
            rfdetr_botsort.process_frame_batch(
                state,
                [(10, frame_a), (20, frame_b)],
            )

        self.assertEqual(
            convert.call_args_list,
            [
                unittest.mock.call(frame_a, rfdetr_botsort.cv2.COLOR_BGR2RGB),
                unittest.mock.call(frame_b, rfdetr_botsort.cv2.COLOR_BGR2RGB),
            ],
        )
        model.predict.assert_called_once_with(
            [rgb_a, rgb_b],
            threshold=0.1,
            include_source_image=False,
        )
        self.assertEqual(
            process.call_args_list,
            [
                unittest.mock.call(state, 10, frame_a, detection_a),
                unittest.mock.call(state, 20, frame_b, detection_b),
            ],
        )
        self.assertEqual(config.result.processed_frames, 7)
        self.assertEqual(progress, [7])

    def test_botsort_process_batch_rejects_non_list_results(self):
        model = MagicMock()
        model.predict.return_value = object()
        state = rfdetr_botsort.RFDetrRunState(
            config=RFDetrBotsortConfig(
                video=Path("video.mp4"),
                device_variant="cpu",
                device="cpu",
                batch_size=1,
            ),
            runtime=rfdetr_botsort.RFDetrRuntime(model=model),
            metadata=VideoMetadata(30.0, 120, 80, 61),
            sampling=SamplingConfig(1, 30.0, 61),
            tracker=MagicMock(),
            lost_track_buffer=30,
            line_track_cache={},
            line_zone=None,
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "^RF-DETR returned an unexpected result for batched input$",
        ):
            rfdetr_botsort.process_frame_batch(
                state,
                [(0, np.zeros((1, 1, 3), dtype=np.uint8))],
            )


if __name__ == "__main__":
    unittest.main()
