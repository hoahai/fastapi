import { useState, type ReactNode } from "react";
import { UserRound } from "lucide-react";

type LeaveSpherePtoEmployeeHeaderProps = {
  employeeName: string;
  pictureUrl?: string | null;
  title: ReactNode;
  subtitle?: ReactNode;
  className?: string;
  titleClassName?: string;
  subtitleClassName?: string;
};

export function LeaveSpherePtoEmployeeHeader({
  employeeName,
  pictureUrl,
  title,
  subtitle,
  className,
  titleClassName,
  subtitleClassName,
}: LeaveSpherePtoEmployeeHeaderProps) {
  const [imageError, setImageError] = useState(false);
  const showPicture = Boolean(pictureUrl) && !imageError;

  return (
    <div className={`flex items-center gap-2 ${className ?? ""}`.trim()}>
      <span className="inline-flex size-7 shrink-0 items-center justify-center overflow-hidden rounded-full border border-blue-100 bg-blue-50 text-blue-700">
        {showPicture ? (
          <img
            src={pictureUrl || undefined}
            alt={employeeName}
            className="size-full object-cover"
            loading="lazy"
            onError={() => setImageError(true)}
          />
        ) : (
          <UserRound className="size-4" />
        )}
      </span>
      <div className="min-w-0">
        <p className={`text-sm font-medium text-slate-900 ${titleClassName ?? ""}`.trim()}>{title}</p>
        {subtitle ? (
          <p className={`whitespace-nowrap text-xs text-slate-500 ${subtitleClassName ?? ""}`.trim()}>{subtitle}</p>
        ) : null}
      </div>
    </div>
  );
}
