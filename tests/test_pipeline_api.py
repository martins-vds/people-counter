import unittest
from pathlib import Path

from people_counter.config import RFDetrBotsortConfig, RTDetrOsnetConfig
from people_counter.models import RunResult
from people_counter.pipelines.rfdetr_botsort import run as run_botsort
from people_counter.pipelines.rtdetr_osnet import run as run_rtdetr


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


if __name__ == "__main__":
    unittest.main()
