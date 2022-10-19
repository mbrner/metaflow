from warnings import warn

from metaflow.decorators import FlowDecorator
from metaflow import current

# TODO (savin): Lift this decorator up since it's also used by Argo now
class ScheduleDecorator(FlowDecorator):
    """
    Specifies the times when the flow should be run when running on a
    production scheduler.

    Parameters
    ----------
    hourly : bool
        Run the workflow hourly (default: False).
    daily : bool
        Run the workflow daily (default: True).
    weekly : bool
        Run the workflow weekly (default: False).
    cron : str
        Run the workflow at [a custom Cron schedule](https://docs.aws.amazon.com/eventbridge/latest/userguide/scheduled-events.html#cron-expressions)
        specified by this expression.
    """

    name = "schedule"
    defaults = {"cron": None,
                "weekly": False,
                "daily": True,
                "hourly": False,
                "active": "always" # ['always', 'never', 'on_production']
    }

    options = {
        "schedule": dict(
            default=None,
            show_default=True,
            help="Override 'active' value for the schedule decorator.",
        )
    }

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        active = options["schedule"]
        active = self.attributes["active"] if active is None else active
        if active.lower() in ['off', 'never', 'false']:
            active = False
        elif active.lower() in ['on', 'always', 'true']:
            active = True
        elif active.lower() == 'on_production':
            is_production = current.get('is_production', default=None)
            if is_production is None:
                warn(
                    Warning(
                        "Activation of the schedule was set to 'on_production'"
                        " but there is no information about the project branch"
                        " in the 'current' object. This is probably due to "
                        " the fact that no @project decorator is used at all or it is"
                        " used before the @schedule decorator. Correct order:"
                        " @schedule(..., active='on_production')"
                        " @project(...)"
                        " class YourFlow(FlowSpec):"
                    ),
                    stacklevel=1,
                )
                active = False
            else:
                active = is_production
        else:
            raise ValueError(f"Unknown value for {active} for @schuled(active=...)/--schedule=...")

        # Currently supports quartz cron expressions in UTC as defined in
        # https://docs.aws.amazon.com/eventbridge/latest/userguide/scheduled-events.html#cron-expressions
        if not active:
            self.schedule = None
        elif self.attributes["cron"]:
            self.schedule = self.attributes["cron"]
        elif self.attributes["weekly"]:
            self.schedule = "0 0 ? * SUN *"
        elif self.attributes["hourly"]:
            self.schedule = "0 * * * ? *"
        elif self.attributes["daily"]:
            self.schedule = "0 0 * * ? *"
        else:
            self.schedule = None
