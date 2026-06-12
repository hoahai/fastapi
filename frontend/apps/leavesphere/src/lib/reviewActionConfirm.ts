export type LeaveSphereReviewAction = "approve" | "reject" | "cancel" | "revert";
export type LeaveSphereReviewerRole = "manager" | "admin";

export type LeaveSphereReviewActionConfirmCopy = {
  title: string;
  description: string;
  confirmLabel: string;
  noteHelpText?: string;
};

export function getLeaveSphereReviewActionConfirmCopy(
  action: LeaveSphereReviewAction,
  reviewerRole: LeaveSphereReviewerRole,
): LeaveSphereReviewActionConfirmCopy {
  const reviewerLabel = reviewerRole === "manager" ? "manager" : "admin";

  if (action === "approve") {
    return {
      title: "Approve Request?",
      description: `This will approve the PTO request and apply the current ${reviewerLabel} note.`,
      confirmLabel: "Approve",
      noteHelpText: "Optional. Add context for why this request is being approved.",
    };
  }

  if (action === "reject") {
    return {
      title: "Reject Request?",
      description: `This will reject the PTO request and apply the current ${reviewerLabel} note.`,
      confirmLabel: "Reject",
      noteHelpText: "Optional. Explain why this request is being rejected.",
    };
  }

  if (action === "cancel") {
    return {
      title: "Cancel Request?",
      description: `This will cancel the PTO request and apply the current ${reviewerLabel} note.`,
      confirmLabel: "Cancel request",
      noteHelpText: "Optional. Add context for why this request is being cancelled.",
    };
  }

  return {
    title: "Revert Decision?",
    description: "This will revert the PTO request back to pending and record the note below.",
    confirmLabel: "Revert",
    noteHelpText: "Optional. Add a reason for reverting this decision.",
  };
}
