import { type CacheStatusChipProps, CacheStatusChip } from "../status/CacheStatusChip";
import { cn } from "../utils/cn";

export interface ModalCacheFooterProps extends CacheStatusChipProps {
  className?: string;
}

export function ModalCacheFooter({
  className,
  containerClassName,
  ...chipProps
}: ModalCacheFooterProps) {
  return (
    <footer className={cn("shrink-0 border-t border-slate-100 bg-white px-0 pt-2 pb-[calc(1rem+env(safe-area-inset-bottom))]", className)}>
      <CacheStatusChip
        {...chipProps}
        containerClassName={cn("pointer-events-auto", containerClassName)}
      />
    </footer>
  );
}
