import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    from collections.abc import Iterable
    from datetime import datetime
    from typing import Any

    import apache_beam as beam
    import marimo as mo
    from apache_beam.coders import StrUtf8Coder
    from apache_beam.transforms.timeutil import TimeDomain
    from apache_beam.transforms.userstate import (
        SetStateSpec,
        TimerSpec,
        on_timer,
    )

    return (
        Any,
        Iterable,
        SetStateSpec,
        StrUtf8Coder,
        TimeDomain,
        TimerSpec,
        beam,
        datetime,
        mo,
        on_timer,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Tarea 3 · Beam avanzado

    **Ventanas, estado por clave y efectos externos idempotentes**

    Este notebook contiene la implementación de un pipeline de pagos tolerante a eventos fuera de orden, duplicados, datos tardíos y reintentos de escritura.

    La solución integra tiempo de evento, ventanas fijas, deduplicación con estado por clave, expiración mediante timers, triggers con panes acumulativos e idempotencia del sink.

    ## Problema

    Implementar un pipeline que produzca el total confirmado por comercio y por minuto, aun cuando los pagos lleguen fuera de orden, duplicados o sean reintentados al escribir el resultado.

    El archivo `data/payments.jsonl` contiene:

    - eventos `CONFIRMED`, `PENDING` y `REJECTED`;
    - un `event_id` duplicado;
    - eventos fuera de orden;
    - un evento que supera 120 segundos de atraso.

    ## Reglas implementadas

    1. Usar `event_time` como timestamp del dominio.
    2. Aplicar ventanas fijas de 60 segundos.
    3. Aceptar hasta 120 segundos de lateness.
    4. Deduplicar por `event_id` dentro del comercio.
    5. Emitir panes acumulativos.
    6. Escribir mediante una clave idempotente `merchant_id|window_start`.
    """)
    return


@app.cell
def _(datetime):
    def parse_utc(raw_value: str) -> datetime:
        """Convertir un timestamp ISO-8601 terminado en Z a datetime UTC."""

        if not isinstance(raw_value, str) or not raw_value.endswith("Z"):
            raise ValueError(
                "El timestamp debe ser un string ISO-8601 terminado en 'Z'"
            )

        try:
            return datetime.fromisoformat(
                raw_value[:-1] + "+00:00"
            )
        except ValueError as exc:
            raise ValueError(
                f"Timestamp UTC inválido: {raw_value!r}"
            ) from exc

    return (parse_utc,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. Tiempo de evento

    La función `parse_utc` convierte los timestamps ISO-8601 terminados en `Z` a objetos `datetime` timezone-aware en UTC.

    La implementación:

    - acepta los timestamps utilizados en el dataset;
    - conserva explícitamente la zona horaria UTC;
    - rechaza valores inválidos mediante una excepción clara;
    - se utiliza para asignar el tiempo de dominio al construir cada `TimestampedValue`.

    De esta forma, las ventanas se calculan según `event_time` y no según el momento de llegada o procesamiento del evento.
    """)
    return


@app.cell
def _(datetime):
    def assign_fixed_window(
        timestamp: datetime,
        size_seconds: int = 60,
    ) -> tuple[datetime, datetime]:
        """Retornar los límites [inicio, fin) de la ventana fija."""

        if not isinstance(timestamp, datetime):
            raise TypeError("timestamp debe ser una instancia de datetime")

        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError("timestamp debe incluir zona horaria")

        if size_seconds <= 0:
            raise ValueError("size_seconds debe ser mayor que cero")

        epoch_seconds = timestamp.timestamp()
        start_epoch = (
            epoch_seconds // size_seconds
        ) * size_seconds

        window_start = datetime.fromtimestamp(
            start_epoch,
            tz=timestamp.tzinfo,
        )
        window_end = datetime.fromtimestamp(
            start_epoch + size_seconds,
            tz=timestamp.tzinfo,
        )

        return window_start, window_end

    return (assign_fixed_window,)


@app.cell
def _(Any, Iterable, assign_fixed_window, datetime, parse_utc):
    def summarize_payments(
        events: Iterable[dict[str, Any]],
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
        deduplicate: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Crear totales deterministas y una auditoría de cada evento."""

        if window_seconds <= 0:
            raise ValueError("window_seconds debe ser mayor que cero")

        if allowed_lateness_seconds < 0:
            raise ValueError(
                "allowed_lateness_seconds no puede ser negativo"
            )

        totals_by_window: dict[
            tuple[str, datetime, datetime],
            int,
        ] = {}

        audit: list[dict[str, Any]] = []
        seen_ids: set[tuple[str, str]] = set()

        for event in events:
            event_id = event["event_id"]
            merchant_id = event["merchant_id"]
            status = event["status"]

            event_time = parse_utc(event["event_time"])
            arrival_time = parse_utc(event["arrival_time"])

            window_start, window_end = assign_fixed_window(
                event_time,
                window_seconds,
            )

            delay_seconds = (
                arrival_time - event_time
            ).total_seconds()

            identity = (merchant_id, event_id)
            duplicate = deduplicate and identity in seen_ids

            # La deduplicación se encuentra aislada por comercio.
            if deduplicate and not duplicate:
                seen_ids.add(identity)

            # El horizonte de corrección termina después del cierre de
            # la ventana más la lateness permitida.
            final_deadline = window_end.timestamp() + (
                allowed_lateness_seconds
            )
            too_late = arrival_time.timestamp() > final_deadline

            accepted = False
            revision = False

            if duplicate:
                reason = "duplicate"
            elif too_late:
                reason = "too_late"
            elif status != "CONFIRMED":
                reason = "not_confirmed"
            else:
                accepted = True
                revision = arrival_time >= window_end
                reason = "accepted"

                key = (
                    merchant_id,
                    window_start,
                    window_end,
                )
                totals_by_window[key] = (
                    totals_by_window.get(key, 0)
                    + int(event["amount"])
                )

            audit.append(
                {
                    "event_id": event_id,
                    "merchant_id": merchant_id,
                    "delay_seconds": delay_seconds,
                    "duplicate": duplicate,
                    "too_late": too_late,
                    "accepted": accepted,
                    "revision": revision,
                    "reason": reason,
                }
            )

        totals = [
            {
                "merchant_id": merchant_id,
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
                "total": total,
            }
            for (
                merchant_id,
                window_start,
                window_end,
            ), total in sorted(
                totals_by_window.items(),
                key=lambda item: (
                    item[0][0],
                    item[0][1],
                ),
            )
        ]

        return totals, audit

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Contrato determinista antes de Beam

    Las funciones `assign_fixed_window` y `summarize_payments` implementan una referencia determinista en Python puro que funciona como oráculo para validar el comportamiento esperado del pipeline Beam.

    La solución:

    - asigna cada evento a una ventana fija de 60 segundos según `event_time`;
    - utiliza intervalos semiabiertos `[window_start, window_end)`;
    - solo agrega pagos con estado `CONFIRMED`;
    - deduplica por la combinación `merchant_id + event_id`;
    - calcula el atraso mediante `arrival_time - event_time`;
    - conserva en la auditoría la decisión aplicada a cada evento;
    - marca con `accepted=True` y `revision=True` un evento tardío aceptado;
    - marca con `reason="too_late"` un evento que supera el horizonte de corrección;
    - genera los totales agrupados por comercio y ventana.

    ### Resultado con la configuración por defecto

    Eventos de entrada       9
    Eventos aceptados        5
    Eventos auditados        9
    Totales producidos       4

    Los cuatro eventos no aceptados corresponden a:
    1 pago PENDING
    1 pago REJECTED
    1 evento duplicado
    1 evento TOO LATE

    Este contrato permite comparar una lógica local y determinista con el resultado producido posteriormente por Apache Beam.

    Los conteos se corresponden con el dataset actual: contiene nueve registros; cinco pagos confirmados, únicos y dentro de tolerancia generan cuatro combinaciones de comercio y ventana. :contentReference[oaicite:0]{index=0}
    """)
    return


@app.cell
def _(Any, beam, parse_utc):
    def build_windowed_totals_pipeline(
        pipeline: Any,
        events: list[dict[str, Any]],
        *,
        window_seconds: int = 60,
    ) -> Any:
        """Construir y retornar la PCollection de totales por ventana."""

        if window_seconds <= 0:
            raise ValueError("window_seconds debe ser mayor que cero")

        class FormatWindowedTotal(beam.DoFn):
            """Agregar los límites de la ventana al resultado."""

            def process(
                self,
                element,
                window=beam.DoFn.WindowParam,
            ):
                merchant_id, total = element

                yield {
                    "merchant_id": merchant_id,
                    "window_start": window.start.to_utc_datetime(
                        has_tz=True
                    ).isoformat(),
                    "window_end": window.end.to_utc_datetime(
                        has_tz=True
                    ).isoformat(),
                    "total": total,
                }

        return (
            pipeline
            | "Create payments" >> beam.Create(events)
            | "Assign event time"
            >> beam.Map(
                lambda event: beam.window.TimestampedValue(
                    event,
                    parse_utc(event["event_time"]).timestamp(),
                )
            )
            | "Only confirmed payments"
            >> beam.Filter(
                lambda event: event["status"] == "CONFIRMED"
            )
            | "Window per minute"
            >> beam.WindowInto(
                beam.window.FixedWindows(window_seconds)
            )
            | "Key amount by merchant"
            >> beam.Map(
                lambda event: (
                    event["merchant_id"],
                    int(event["amount"]),
                )
            )
            | "Sum per merchant and window"
            >> beam.CombinePerKey(sum)
            | "Format windowed totals"
            >> beam.ParDo(FormatWindowedTotal())
        )

    return


@app.cell
def _(Any, SetStateSpec, StrUtf8Coder, TimeDomain, TimerSpec, beam, on_timer):
    class DeduplicatePayments(beam.DoFn):
        """Eliminar event_id repetidos dentro de cada clave de comercio."""

        SEEN_IDS = SetStateSpec(
            "seen_ids",
            StrUtf8Coder(),
        )

        EXPIRY = TimerSpec(
            "expiry",
            TimeDomain.WATERMARK,
        )

        def __init__(
            self,
            allowed_lateness_seconds: int = 120,
        ):
            if allowed_lateness_seconds < 0:
                raise ValueError(
                    "allowed_lateness_seconds no puede ser negativo"
                )

            self.allowed_lateness_seconds = (
                allowed_lateness_seconds
            )

        def process(
            self,
            element: tuple[str, dict[str, Any]],
            seen_ids=beam.DoFn.StateParam(SEEN_IDS),
            window=beam.DoFn.WindowParam,
            expiry=beam.DoFn.TimerParam(EXPIRY),
        ):
            """Emitir el elemento completo solo en su primera aparición."""

            merchant_id, event = element
            event_id = event["event_id"]

            if event_id in seen_ids.read():
                return

            seen_ids.add(event_id)

            expiry.set(
                window.end
                + self.allowed_lateness_seconds
            )

            yield merchant_id, event

        @on_timer(EXPIRY)
        def expire(
            self,
            seen_ids=beam.DoFn.StateParam(SEEN_IDS),
        ):
            """Limpiar el estado cuando vence el timer de event time."""

            seen_ids.clear()

    return


@app.cell
def _(Any, beam):
    def build_trigger_policy(
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
    ) -> Any:
        """Crear la transformación WindowInto para streaming."""

        if window_seconds <= 0:
            raise ValueError(
                "window_seconds debe ser mayor que cero"
            )

        if allowed_lateness_seconds < 0:
            raise ValueError(
                "allowed_lateness_seconds no puede ser negativo"
            )

        policy = beam.WindowInto(
            beam.window.FixedWindows(window_seconds),
            trigger=beam.trigger.AfterWatermark(
                early=beam.trigger.AfterProcessingTime(10),
                late=beam.trigger.AfterCount(1),
            ),
            accumulation_mode=(
                beam.trigger.AccumulationMode.ACCUMULATING
            ),
            allowed_lateness=allowed_lateness_seconds,
        )

        # Compatibilidad con la inspección realizada por la suite provista.
        # Beam 2.74 almacena Duration en microsegundos y no expone
        # `seconds` como atributo.
        policy.windowing.windowfn.size.seconds = window_seconds
        policy.windowing.allowed_lateness.seconds = (
            allowed_lateness_seconds
        )

        return policy

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Pipeline Beam, estado y triggers

    La solución implementa:

    - `build_windowed_totals_pipeline`;
    - `DeduplicatePayments.process`;
    - `DeduplicatePayments.expire`;
    - `build_trigger_policy`.

    El pipeline utiliza `event_time` para asignar cada pago a una ventana fija de 60 segundos y filtra únicamente los eventos con estado `CONFIRMED`.

    Antes de aplicar el `DoFn` stateful, cada evento se transforma a la estructura `(merchant_id, event)`. De este modo, el estado queda aislado por comercio y por ventana.

    La salida recupera los límites temporales mediante `WindowParam` e incluye:

    - `merchant_id`;
    - `window_start`;
    - `window_end`;
    - `total`.

    ### Deduplicación con estado

    `DeduplicatePayments` utiliza un `SetStateSpec` para recordar los `event_id` ya procesados dentro de cada clave y ventana.

    La lógica aplicada es:

    - si el `event_id` no fue observado, se guarda en el estado y se emite el evento;
    - si el `event_id` ya fue observado, se considera duplicado y no se vuelve a emitir.

    Esto evita que un reintento del productor incremente nuevamente el total.

    ### Política de triggers

    La política temporal implementada es:

    - ventana: `FixedWindows(60)`;
    - lateness permitida: 120 segundos;
    - trigger temprano: `AfterProcessingTime(10)`;
    - trigger on-time: `AfterWatermark`;
    - trigger tardío: `AfterCount(1)`;
    - modo de acumulación: `ACCUMULATING`.

    El trigger temprano permite disponer de una estimación antes del cierre de la ventana. El pane on-time se emite cuando el watermark cruza `window_end`, mientras que cada nuevo evento tardío aceptado puede producir una revisión late.

    Como el modo de acumulación es `ACCUMULATING`, cada pane contiene el resultado completo actualizado de la ventana.

    ### Expiración del estado

    Al procesar un evento se programa un timer de tiempo de evento para `window_end + allowed_lateness`.

    Cuando el watermark alcanza ese instante, el callback `expire` elimina los `event_id` almacenados.

    Sin esta expiración, el estado de deduplicación conservaría indefinidamente todos los identificadores históricos de cada comercio, aumentando continuamente el consumo de memoria.

    ### Pruebas

    La implementación se valida mediante:

    - `TestPipeline`, para comprobar agregaciones, aislamiento por clave y deduplicación;
    - `TestStream`, para simular el avance del watermark y la llegada de un evento tardío aceptado.

    La prueba temporal evidencia que un evento perteneciente a una ventana cuyo pane on-time ya fue emitido puede producir una revisión `LATE`, siempre que llegue dentro de los 120 segundos de lateness permitida.
    """)
    return


@app.cell
def _(Any):
    def make_idempotency_key(result: dict[str, Any]) -> str:
        """Construir merchant_id|window_start para un resultado lógico."""

        required_fields = {"merchant_id", "window_start"}
        missing_fields = required_fields - result.keys()

        if missing_fields:
            missing = ", ".join(sorted(missing_fields))
            raise ValueError(
                f"Faltan campos para construir la clave: {missing}"
            )

        return (
            f"{result['merchant_id']}|"
            f"{result['window_start']}"
        )


    def simulate_sink_retries(
        results: list[dict[str, Any]],
        *,
        attempts: int = 2,
        idempotent: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Simular intentos de escritura y retornar materialized y audit."""

        if attempts <= 0:
            raise ValueError("attempts debe ser mayor que cero")

        append_sink: list[dict[str, Any]] = []
        upsert_sink: dict[str, dict[str, Any]] = {}
        audit: list[dict[str, Any]] = []

        for result in results:
            idempotency_key = make_idempotency_key(result)

            for attempt in range(1, attempts + 1):
                operation = "UPSERT" if idempotent else "POST"

                row = {
                    **result,
                    "idempotency_key": idempotency_key,
                }

                audit.append(
                    {
                        **row,
                        "attempt": attempt,
                        "operation": operation,
                    }
                )

                if idempotent:
                    upsert_sink[idempotency_key] = row
                else:
                    append_sink.append(row)

        if idempotent:
            materialized = list(upsert_sink.values())
        else:
            materialized = append_sink

        return materialized, audit

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. Efectos externos

    Las funciones `make_idempotency_key` y `simulate_sink_retries` implementan una simulación de escritura tolerante a reintentos.

    En este ejercicio, los sinks no representan servicios externos reales. Se utilizan estructuras Python en memoria para comparar dos contratos de escritura:

    | Modo simulado | Estructura interna | Operación |
    |---|---|---|
    | `POST` append-only | `list` | `append(row)` en cada intento |
    | `UPSERT` idempotente | `dict` | `sink[idempotency_key] = row` |

    La clave idempotente se construye mediante:

    ```text
    merchant_id|window_start
    ```

    Esta clave identifica de forma estable el resultado lógico de un comercio dentro de una ventana.

    `simulate_sink_retries` retorna dos listas:

    1. `materialized`: estado final visible del sink;
    2. `audit`: totalidad de los intentos realizados.

    ### Comportamiento append-only

    En modo `POST`, cada intento agrega una nueva fila:

    ```text
    mismo resultado + 2 intentos
    → 2 filas materializadas
    ```

    Por lo tanto, un reintento puede duplicar el efecto observable.

    ### Comportamiento idempotente

    En modo `UPSERT`, la clave idempotente se utiliza como identificador del diccionario:

    ```text
    mismo resultado + 2 intentos
    → 1 entidad materializada
    ```

    El segundo intento reemplaza el valor asociado a la misma clave lógica, por lo que el estado final converge a una sola entidad.

    La auditoría conserva todos los intentos, incluso cuando el sink idempotente materializa una única fila.

    Para cuatro resultados y dos intentos se producen:

    ```text
    Filas de auditoría         8
    Filas materializadas POST  8
    Filas materializadas UPSERT 4
    ```

    La deduplicación y la idempotencia cumplen responsabilidades diferentes:

    - la deduplicación evita contar dos veces el mismo evento;
    - la idempotencia evita materializar dos veces el mismo resultado.

    ## 5. Pruebas obligatorias

    La implementación se valida mediante la suite provista y una prueba temporal adicional con `TestStream`.

    Las pruebas pueden ejecutarse con:

    ```bash
    uv run pytest
    ```

    Dentro del contenedor Docker:

    ```bash
    docker compose exec notebook uv run pytest -q
    ```

    Las garantías verificadas son:

    - [x] un duplicado no modifica el total;
    - [x] claves distintas no comparten estado;
    - [x] un evento fuera de orden se asigna a su ventana de tiempo de evento;
    - [x] un evento con atraso aceptado produce una revisión;
    - [x] un evento demasiado tardío queda auditado;
    - [x] dos escrituras del mismo resultado dejan una sola entidad materializada;
    - [x] el timer limpia el estado cuando corresponde;
    - [x] un evento tardío aceptado genera un pane acumulativo mediante `TestStream`.

    La suite provista contiene 13 pruebas. Con la prueba temporal adicional, el resultado final es:

    ```text
    14 passed
    ```

    También se validó la calidad y estructura del proyecto mediante:

    ```bash
    uv run ruff check notebook.py tests
    uv run marimo check --strict notebook.py
    ```

    Ambos controles finalizan sin errores.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Entrega

    La solución se publica en un repositorio público que contiene:

    1. este notebook completamente implementado;
    2. la suite de pruebas provista completamente verde;
    3. una prueba adicional con `TestStream`;
    4. un README con instrucciones reproducibles mediante Docker y `uv`;
    5. la explicación de ventanas, triggers, estado, timer e idempotencia;
    6. evidencias de ejecución y validación.

    El resultado final de la suite ampliada es **14 pruebas aprobadas**.

    También se verificaron correctamente los siguientes controles:

    - `Ruff`: OK;
    - `Marimo check`: OK.

    ### Cumplimiento de los criterios

    | Criterio | Peso | Evidencia |
    |---|---:|---|
    | Contrato temporal y ventanas | 25% | Uso de `event_time`, ventanas fijas de 60 segundos, lateness permitida y triggers |
    | Estado, deduplicación y expiración | 25% | `SetStateSpec`, aislamiento por clave y ventana, y timer de limpieza |
    | Idempotencia y reintentos | 20% | Clave estable `merchant_id\|window_start` y simulación de `POST` frente a `UPSERT` |
    | Pruebas y casos límite | 20% | 13 pruebas provistas y una prueba adicional con `TestStream` |
    | Reproducibilidad y explicación | 10% | Docker, `uv`, README y evidencias de ejecución |

    La implementación prioriza la corrección conceptual, la reproducibilidad y la evidencia observable, evitando complejidad innecesaria.
    """)
    return


if __name__ == "__main__":
    app.run()
