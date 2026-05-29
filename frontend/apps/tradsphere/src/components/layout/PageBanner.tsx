import { PageBanner as SharedPageBanner } from "@shell/components/layout/PageBanner";

type SharedPageBannerProps = Parameters<typeof SharedPageBanner>[0];

export function PageBanner(props: SharedPageBannerProps) {
  return <SharedPageBanner gradientVariant="tradsphere" {...props} />;
}
