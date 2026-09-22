"""
surge_pricing_logic_17.py
Pure, explainable pricing logic for the Lecture 17 classroom simulation.

This is an educational model, not Uber's proprietary pricing algorithm.
"""

from dataclasses import dataclass


MIN_MULTIPLIER = 1.0
MAX_MULTIPLIER = 2.5
MAX_CHANGE_PER_DECISION = 0.20


@dataclass(frozen=True)
class PricingInputs:
    demand_window: int
    available_drivers: int
    rain_intensity: float
    traffic_index: float
    event_intensity: float


@dataclass(frozen=True)
class PricingDecision:
    demand_supply_ratio: float
    demand_adjustment: float
    weather_adjustment: float
    traffic_adjustment: float
    event_adjustment: float
    raw_multiplier: float
    final_multiplier: float
    reason: str


def clamp(value, lower, upper):
    return max(lower, min(upper, value))


def calculate_surge(inputs, previous_multiplier=1.0):
    """Calculate a capped, rate-limited multiplier with visible components."""
    safe_supply = max(inputs.available_drivers, 1)
    ratio = inputs.demand_window / safe_supply

    demand_adjustment = max(ratio - 1.0, 0.0) * 0.35
    weather_adjustment = clamp(inputs.rain_intensity, 0.0, 1.0) * 0.20
    traffic_adjustment = max(
        clamp(inputs.traffic_index, 0.0, 1.0) - 0.50,
        0.0,
    ) * 0.30
    event_adjustment = clamp(inputs.event_intensity, 0.0, 1.0) * 0.25

    raw_multiplier = (
        1.0
        + demand_adjustment
        + weather_adjustment
        + traffic_adjustment
        + event_adjustment
    )
    capped_multiplier = clamp(
        raw_multiplier,
        MIN_MULTIPLIER,
        MAX_MULTIPLIER,
    )

    change = clamp(
        capped_multiplier - previous_multiplier,
        -MAX_CHANGE_PER_DECISION,
        MAX_CHANGE_PER_DECISION,
    )
    final_multiplier = clamp(
        previous_multiplier + change,
        MIN_MULTIPLIER,
        MAX_MULTIPLIER,
    )

    components = []
    if demand_adjustment > 0:
        components.append("demand exceeds supply")
    if weather_adjustment > 0.05:
        components.append("rain")
    if traffic_adjustment > 0:
        components.append("traffic")
    if event_adjustment > 0:
        components.append("nearby event")
    reason = ", ".join(components) if components else "normal conditions"

    return PricingDecision(
        demand_supply_ratio=round(ratio, 3),
        demand_adjustment=round(demand_adjustment, 3),
        weather_adjustment=round(weather_adjustment, 3),
        traffic_adjustment=round(traffic_adjustment, 3),
        event_adjustment=round(event_adjustment, 3),
        raw_multiplier=round(raw_multiplier, 2),
        final_multiplier=round(final_multiplier, 2),
        reason=reason,
    )
