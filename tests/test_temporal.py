import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
)
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream as BeamTestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue


def test_teststream_accepts_late_event_within_lateness(solution):
    """Un evento tardío dentro de la tolerancia corrige el total."""

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    stream = (
        BeamTestStream()
        .advance_watermark_to(0)
        .add_elements(
            [
                TimestampedValue(("m-a", 10), 5),
                TimestampedValue(("m-a", 20), 20),
            ]
        )
        .advance_watermark_to(60)
        .add_elements(
            [
                # Pertenece a [0, 60), pero llega después del
                # watermark de cierre. Sigue dentro de los
                # 120 segundos de lateness permitida.
                TimestampedValue(("m-a", 30), 40),
            ]
        )
        .advance_watermark_to_infinity()
    )

    with BeamTestPipeline(options=options) as pipeline:
        totals = (
            pipeline
            | "Temporal input" >> stream
            | "Temporal policy"
            >> solution.build_trigger_policy(
                window_seconds=60,
                allowed_lateness_seconds=120,
            )
            | "Sum temporal values"
            >> beam.CombinePerKey(sum)
        )

        assert_that(
            totals,
            equal_to(
                [
                    ("m-a", 30),
                    ("m-a", 60),
                ]
            ),
        )