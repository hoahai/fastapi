export type SharedLoadingContractState = {
  pageInitializing?: boolean;
  pageRefreshing?: boolean;
  cacheChipRefreshing?: boolean;
  sectionLoading?: boolean;
  searchLoading?: boolean;
};

export type SharedLoadingContractMessages = {
  pageInitializing?: string;
  pageRefreshing?: string;
  cacheChipRefreshing?: string;
  sectionLoading?: string;
  searchLoading?: string;
  defaultPageMessage?: string;
  defaultSectionMessage?: string;
};

export type ResolvedLoadingContract = {
  pageOverlayActive: boolean;
  pageOverlayMessage: string;
  sectionOverlayActive: boolean;
  sectionOverlayMessage: string;
};

type LoadingStageName =
  | "pageInitializing"
  | "cacheChipRefreshing"
  | "pageRefreshing"
  | "sectionLoading"
  | "searchLoading";

function resolveFirstActiveStage(
  order: LoadingStageName[],
  state: SharedLoadingContractState,
): LoadingStageName | null {
  for (const stage of order) {
    if (state[stage]) {
      return stage;
    }
  }
  return null;
}

export function resolveSharedLoadingContract(
  state: SharedLoadingContractState,
  messages: SharedLoadingContractMessages = {},
): ResolvedLoadingContract {
  const pageStage = resolveFirstActiveStage(
    ["pageInitializing", "cacheChipRefreshing", "pageRefreshing"],
    state,
  );
  const pageOverlayActive = pageStage !== null;
  const pageOverlayMessage = pageStage
    ? messages[pageStage] ?? messages.defaultPageMessage ?? "Loading..."
    : messages.defaultPageMessage ?? "Loading...";

  const sectionStage = pageOverlayActive
    ? null
    : resolveFirstActiveStage(["sectionLoading", "searchLoading"], state);
  const sectionOverlayActive = sectionStage !== null;
  const sectionOverlayMessage = sectionStage
    ? messages[sectionStage] ?? messages.defaultSectionMessage ?? "Loading..."
    : messages.defaultSectionMessage ?? "Loading...";

  return {
    pageOverlayActive,
    pageOverlayMessage,
    sectionOverlayActive,
    sectionOverlayMessage,
  };
}
