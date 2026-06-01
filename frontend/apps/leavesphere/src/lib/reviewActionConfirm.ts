export type LeaveSphereReviewAction = "approve" | "reject" | "cancel" | "revert";
export type LeaveSphereReviewerRole = "manager" | "admin";

export type LeaveSphereReviewActionConfirmCopy = {
  title: string;
  description: string;
  confirmLabel: string;
};

export function getLeaveSphereReviewActionConfirmCopy(
  action: LeaveSphereReviewAction,
  reviewerRole: LeaveSphereReviewerRole,
): LeaveSphereReviewActionConfirmCopy {
  const reviewerLabel = reviewerRole === "manager" ? "manager" : "admin";

  if (action === "approve") {
    return {
      title: "Approve request?",
      description: `This will approve the PTO request and apply the current ${reviewerLabel} note.`,
      confirmLabel: "Approve",
    };
  }

  if (action === "reject") {
    return {
      title: "Reject request?",
      description: `This will reject the PTO request and apply the current ${reviewerLabel} note.`,
      confirmLabel: "Reject",
    };
  }

  if (action === "cancel") {
    return {
      title: "Cancel request?",
      description: `This will cancel the PTO request and apply the current ${reviewerLabel} note.`,
      confirmLabel: "Cancel request",
    };
  }

  return {
    title: "Revert decision?",
    description: "This will revert the PTO request back to pending.",
    confirmLabel: "Revert",
  };
}
