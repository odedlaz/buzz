import { Cloud } from "lucide-react";

import { cn } from "@/shared/lib/cn";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/shared/ui/tooltip";

const OTHER_SETUP_LABEL = "From another Buzz setup";

export function OtherSetupAgentMarker({
  className,
  testId,
}: {
  className?: string;
  testId?: string;
}) {
  return (
    <Tooltip disableHoverableContent>
      <TooltipTrigger asChild>
        <span
          aria-label={OTHER_SETUP_LABEL}
          className={cn("inline-flex shrink-0", className)}
          data-testid={testId}
          role="img"
        >
          <Cloud aria-hidden="true" className="h-3 w-3" />
        </span>
      </TooltipTrigger>
      <TooltipContent side="top">{OTHER_SETUP_LABEL}</TooltipContent>
    </Tooltip>
  );
}
