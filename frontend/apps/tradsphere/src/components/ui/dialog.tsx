import * as React from "react";
import * as DialogPrimitive from "@radix-ui/react-dialog";
import { AnimatePresence, motion } from "framer-motion";

import { cn } from "@/lib/utils";

const DIALOG_MOTION_DURATION_SECONDS = 0.3;
const DIALOG_EXIT_CLEANUP_MS = 340;

const dialogOverlayMotionTransition = {
  duration: DIALOG_MOTION_DURATION_SECONDS,
  ease: "easeOut" as const,
};

const dialogContentMotionTransition = {
  duration: DIALOG_MOTION_DURATION_SECONDS,
  ease: [0.22, 1, 0.36, 1] as const,
};

type DialogProps = React.ComponentPropsWithoutRef<typeof DialogPrimitive.Root>;

const DialogStateContext = React.createContext<{ open: boolean }>({ open: false });

function Dialog({ open: openProp, defaultOpen = false, onOpenChange, ...props }: DialogProps) {
  const isControlled = openProp !== undefined;
  const [uncontrolledOpen, setUncontrolledOpen] = React.useState(defaultOpen);
  const open = isControlled ? Boolean(openProp) : uncontrolledOpen;

  const handleOpenChange = React.useCallback(
    (nextOpen: boolean) => {
      if (!isControlled) {
        setUncontrolledOpen(nextOpen);
      }
      onOpenChange?.(nextOpen);
    },
    [isControlled, onOpenChange],
  );

  return (
    <DialogStateContext.Provider value={{ open }}>
      <DialogPrimitive.Root {...props} open={open} onOpenChange={handleOpenChange} />
    </DialogStateContext.Provider>
  );
}

const DialogTrigger = DialogPrimitive.Trigger;
const DialogPortal = DialogPrimitive.Portal;
const DialogClose = DialogPrimitive.Close;

const DialogOverlay = React.forwardRef<
  React.ElementRef<typeof DialogPrimitive.Overlay>,
  React.ComponentPropsWithoutRef<typeof DialogPrimitive.Overlay>
>(({ className, ...props }, ref) => (
  <DialogPrimitive.Overlay
    ref={ref}
    className={cn("fixed inset-0 z-50 bg-black/75 backdrop-blur-[8px]", className)}
    {...props}
  />
));
DialogOverlay.displayName = DialogPrimitive.Overlay.displayName;

const DialogContent = React.forwardRef<
  React.ElementRef<typeof DialogPrimitive.Content>,
  React.ComponentPropsWithoutRef<typeof DialogPrimitive.Content>
>(({ className, children, ...props }, ref) => {
  const { open } = React.useContext(DialogStateContext);
  const [shouldRender, setShouldRender] = React.useState(open);

  React.useEffect(() => {
    if (open) {
      setShouldRender(true);
      return;
    }

    const timeoutId = window.setTimeout(() => {
      setShouldRender(false);
    }, DIALOG_EXIT_CLEANUP_MS);

    return () => window.clearTimeout(timeoutId);
  }, [open]);

  if (!shouldRender) {
    return null;
  }

  return (
    <DialogPortal forceMount>
      <AnimatePresence>
        {shouldRender ? (
          <>
            <DialogOverlay asChild forceMount>
              <motion.div
                onClick={(event) => {
                  event.stopPropagation();
                }}
                initial={{ opacity: 0 }}
                animate={{ opacity: open ? 1 : 0 }}
                exit={{ opacity: 0 }}
                transition={dialogOverlayMotionTransition}
              />
            </DialogOverlay>
            <DialogPrimitive.Content ref={ref} forceMount asChild {...props}>
              <motion.div
                className={cn(
                  "fixed inset-0 z-50 m-auto h-fit w-[calc(100%-2rem)] max-w-lg rounded-xl border border-border bg-white p-6 shadow-soft focus:outline-none",
                  className,
                )}
                initial={{ opacity: 0, scale: 1.1 }}
                animate={{ opacity: open ? 1 : 0, scale: open ? 1 : 1.1 }}
                exit={{ opacity: 0, scale: 1.1 }}
                transition={dialogContentMotionTransition}
              >
                {children}
              </motion.div>
            </DialogPrimitive.Content>
          </>
        ) : null}
      </AnimatePresence>
    </DialogPortal>
  );
});
DialogContent.displayName = DialogPrimitive.Content.displayName;

function DialogHeader({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) {
  return <div className={cn("flex flex-col gap-1.5", className)} {...props} />;
}

function DialogFooter({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) {
  return <div className={cn("mt-6 flex flex-col-reverse gap-2 sm:flex-row sm:justify-end", className)} {...props} />;
}

const DialogTitle = React.forwardRef<
  React.ElementRef<typeof DialogPrimitive.Title>,
  React.ComponentPropsWithoutRef<typeof DialogPrimitive.Title>
>(({ className, ...props }, ref) => (
  <DialogPrimitive.Title ref={ref} className={cn("text-lg font-semibold text-foreground", className)} {...props} />
));
DialogTitle.displayName = DialogPrimitive.Title.displayName;

const DialogDescription = React.forwardRef<
  React.ElementRef<typeof DialogPrimitive.Description>,
  React.ComponentPropsWithoutRef<typeof DialogPrimitive.Description>
>(({ className, ...props }, ref) => (
  <DialogPrimitive.Description
    ref={ref}
    className={cn("text-sm text-muted-foreground", className)}
    {...props}
  />
));
DialogDescription.displayName = DialogPrimitive.Description.displayName;

export {
  Dialog,
  DialogPortal,
  DialogOverlay,
  DialogTrigger,
  DialogClose,
  DialogContent,
  DialogHeader,
  DialogFooter,
  DialogTitle,
  DialogDescription,
};
