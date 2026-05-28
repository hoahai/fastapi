import { type CacheStatusChipProps, CacheStatusChip } from "../status/CacheStatusChip";
import { cn } from "../utils/cn";

export interface PageCacheFooterProps extends CacheStatusChipProps {
  className?: string;
}

export function PageCacheFooter({
  className,
  containerClassName,
  ...chipProps
}: PageCacheFooterProps) {
  return (
    <footer className={cn("bg-transparent px-0", className)}>
      <CacheStatusChip
        {...chipProps}
        containerClassName={cn("pointer-events-auto", containerClassName)}
      />
    </footer>
  );
}
