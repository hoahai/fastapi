export type LeaveSphereApprovalAction = "approve" | "reject" | "cancel" | "revert";

export type LeaveSphereApprovalReviewerRole = "manager" | "admin";

export type LeaveSphereApprovalActionConfirmCopy = {
  title: string;
  description: string;
  confirmLabel: string;
  noteRequired: boolean;
  noteHelpText?: string;
  noteLabel: string;
};

export function getLeaveSphereApprovalActionConfirmCopy(
  action: LeaveSphereApprovalAction,
  _reviewerRole: LeaveSphereApprovalReviewerRole,
): LeaveSphereApprovalActionConfirmCopy {
  if (action === "approve") {
    return {
      title: "Approve PTO request?",
      description: "This will approve the PTO request and record the approver note.",
      confirmLabel: "Approve request",
      noteRequired: false,
      noteHelpText: "Optional. Add context for why this request is being approved.",
      noteLabel: "Approver note / reason",
    };
  }

  if (action === "reject") {
    return {
      title: "Reject PTO request?",
      description: "This will reject the PTO request and record the approver note.",
      confirmLabel: "Confirm rejection",
      noteRequired: true,
      noteHelpText: "Required. Explain why this request is being rejected.",
      noteLabel: "Approver note / reason",
    };
  }

  if (action === "cancel") {
    return {
      title: "Cancel PTO request?",
      description: "This will cancel the PTO request and record the approver note.",
      confirmLabel: "Cancel request",
      noteRequired: true,
      noteHelpText: "Required. Add context for why this request is being cancelled.",
      noteLabel: "Approver note / reason",
    };
  }

  return {
    title: "Revert PTO decision?",
    description: "This will revert the PTO request back to pending and record the approver note.",
    confirmLabel: "Revert",
    noteRequired: false,
    noteHelpText: "Optional. Add a reason for reverting this decision.",
    noteLabel: "Approver note / reason",
  };
}
