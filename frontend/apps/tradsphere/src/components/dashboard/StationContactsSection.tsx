import { CirclePlus, UserPlus } from "lucide-react";

import { ActionIconButton } from "@/components/dashboard/ActionIconButton";

import { StationContactCard, type StationDraftContact } from "./StationContactCard";

interface StationContactsSectionProps {
  contacts: StationDraftContact[];
  isSubmitting: boolean;
  onAddExistingContact: () => void;
  onCreateContact: () => void;
  onEditContact: (index: number) => void;
  onRemoveContact: (index: number) => void;
  onCopyContact: (index: number) => void;
}

export function StationContactsSection({
  contacts,
  isSubmitting,
  onAddExistingContact,
  onCreateContact,
  onEditContact,
  onRemoveContact,
  onCopyContact,
}: StationContactsSectionProps) {
  return (
    <section className="flex min-h-[420px] min-w-0 flex-col space-y-3">
      <div className="flex min-h-10 items-center justify-between gap-3 border-b border-slate-200 pb-2">
        <h3 className="text-sm font-semibold text-slate-800">Contacts</h3>
        <div className="flex flex-wrap items-center justify-end gap-1">
          <ActionIconButton
            icon={<CirclePlus />}
            tooltip="Create contact"
            onClick={onCreateContact}
            disabled={isSubmitting}
          />
          <ActionIconButton
            icon={<UserPlus />}
            tooltip="Add existing contact"
            onClick={onAddExistingContact}
            disabled={isSubmitting}
          />
        </div>
      </div>

      {contacts.length ? (
        <div className="flex-1 space-y-2 overflow-y-auto pr-1">
          {contacts.map((contact, index) => (
            <StationContactCard
              key={`${contact.contactType}:${contact.contactId ?? contact.clientKey ?? index}:${contact.email ?? index}:${index}`}
              contact={contact}
              isSubmitting={isSubmitting}
              onEdit={() => onEditContact(index)}
              onRemove={() => onRemoveContact(index)}
              onCopy={() => onCopyContact(index)}
            />
          ))}
        </div>
      ) : (
        <div className="rounded-md border border-dashed border-slate-300 bg-slate-50 px-3 py-4 text-sm text-slate-500">
          No linked contacts.
        </div>
      )}
    </section>
  );
}
