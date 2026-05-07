import {
  FormRow,
  type FormRowProps,
} from "@shared/components/form/FormRow";
import {
  ReadOnlyField,
  type ReadOnlyFieldProps,
} from "@shared/components/form/ReadOnlyField";

export type LabeledFieldProps = FormRowProps;

export function LabeledField(props: LabeledFieldProps) {
  return <FormRow {...props} />;
}

export function ReadOnlyValue(props: ReadOnlyFieldProps) {
  return <ReadOnlyField {...props} />;
}
