import * as React from "react";
import { Check, Copy } from "lucide-react";

import { HostedCommunityOnboarding } from "@/features/communities/ui/HostedCommunityOnboarding";
import { useCommunityOnboarding } from "@/features/onboarding/communityOnboarding";
import { InviteRedeemForm } from "@/features/onboarding/ui/InviteRedeemForm";
import { OnboardingChrome } from "@/features/onboarding/ui/OnboardingChrome";
import { normalizeNip17Config } from "@/shared/api/nip17Transport";
import {
  OnboardingFooter,
  OnboardingFooterProvider,
} from "@/features/onboarding/ui/OnboardingFooter";
import {
  type OnboardingTransitionDirection,
  OnboardingSlideTransition,
} from "@/features/onboarding/ui/OnboardingSlideTransition";
import { useIdentityQuery } from "@/shared/api/hooks";
import { writeTextToClipboard } from "@/shared/lib/clipboard";
import { pubkeyToNpub } from "@/shared/lib/nostrUtils";
import { useSystemColorScheme } from "@/shared/theme/useSystemColorScheme";
import { Button } from "@/shared/ui/button";
import { Card } from "@/shared/ui/card";
import { Input } from "@/shared/ui/input";
import { StartupWindowDragRegion } from "@/shared/ui/StartupWindowDragRegion";

type WelcomeSetupPage =
  | "welcome"
  | "existing"
  | "join"
  | "member"
  | "nip17"
  | "owned";
type WelcomeTransitionMode = "initial" | OnboardingTransitionDirection;

type WelcomeSetupProps = {
  initialPage?: WelcomeSetupPage;
  initialTransitionMode?: WelcomeTransitionMode;
  onBack: () => void;
};

const COMMUNITY_OPTION_CARD_CLASS =
  "w-full max-w-[320px] items-center px-6 py-4 text-center text-sm font-normal leading-6 text-foreground [--buzz-card-textured-min-height:88px] transition-[filter] duration-150 ease-out hover:brightness-[0.98] focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-foreground/35";
const NIP17_RELAY_IDENTITY = "wss://nip17.buzz.invalid";
const DEFAULT_NIP17_PUBLIC_RELAYS = [
  "wss://relay.damus.io",
  "wss://nos.lol",
  "wss://nostr.mom",
].join("\n");

export function WelcomeSetup({
  initialPage = "welcome",
  initialTransitionMode = "initial",
  onBack,
}: WelcomeSetupProps) {
  const [page, setPage] = React.useState<WelcomeSetupPage>(initialPage);
  const [transitionMode, setTransitionMode] =
    React.useState<WelcomeTransitionMode>(initialTransitionMode);
  // While true, the Builderlab sign-in modal floats over the current page —
  // we only navigate to the hosted stage once sign-in completes, so the page
  // behind the modal never changes out from under the user.
  const [isHostedSignInOpen, setIsHostedSignInOpen] = React.useState(false);
  const [copiedNpub, setCopiedNpub] = React.useState(false);
  const [nip17Name, setNip17Name] = React.useState("Private community");
  const [nip17GatewayPubkey, setNip17GatewayPubkey] = React.useState("");
  const [nip17PublicRelayUrls, setNip17PublicRelayUrls] = React.useState(
    DEFAULT_NIP17_PUBLIC_RELAYS,
  );
  const [nip17Error, setNip17Error] = React.useState<string | null>(null);
  const communityOnboarding = useCommunityOnboarding();
  const identityQuery = useIdentityQuery();
  const systemColorScheme = useSystemColorScheme();
  const npub = identityQuery.data?.pubkey
    ? pubkeyToNpub(identityQuery.data.pubkey)
    : "";
  const npubError = identityQuery.error
    ? identityQuery.error instanceof Error
      ? identityQuery.error.message
      : "Could not load your public key."
    : null;

  const showPage = React.useCallback(
    (nextPage: WelcomeSetupPage, direction?: OnboardingTransitionDirection) => {
      setTransitionMode(
        direction ?? (nextPage === "welcome" ? "backward" : "forward"),
      );
      setPage(nextPage);
    },
    [],
  );

  const startConnection = React.useCallback(
    (relayUrl: string) => {
      communityOnboarding.start({
        source: "first-community",
        firstCommunityPage: page === "member" ? "member" : "join",
        relayUrl,
      });
    },
    [communityOnboarding, page],
  );

  const redeemInvite = React.useCallback(
    (relayUrl: string, code: string, policyReceipt?: string) => {
      communityOnboarding.start({
        source: "first-community",
        firstCommunityPage: page === "member" ? "member" : "join",
        relayUrl,
        inviteCode: code,
        policyReceipt,
      });
    },
    [communityOnboarding, page],
  );
  const startNip17Connection = React.useCallback(() => {
    const transport = normalizeNip17Config({
      relayTransport: "nip17",
      nip17GatewayPubkey: nip17GatewayPubkey.trim(),
      nip17PublicRelayUrls: nip17PublicRelayUrls.split(/\s+/).filter(Boolean),
    });
    if (transport.relayTransport !== "nip17") {
      setNip17Error(
        "Enter a 64-character gateway pubkey and at least one public relay URL.",
      );
      return;
    }
    communityOnboarding.start({
      source: "first-community",
      firstCommunityPage: "join",
      relayUrl: NIP17_RELAY_IDENTITY,
      communityName: nip17Name.trim() || "Private community",
      ...transport,
    });
  }, [
    communityOnboarding,
    nip17GatewayPubkey,
    nip17Name,
    nip17PublicRelayUrls,
  ]);

  const transitionDirection =
    transitionMode === "backward" ? "backward" : "forward";
  const welcomeEffect =
    transitionMode === "backward" ? "line-slide" : "mask-reveal-up";

  return (
    <div
      className="buzz-onboarding-neutral-theme buzz-startup-shell flex h-dvh items-start justify-center overflow-y-auto bg-background px-4 pb-36 pt-[106px] text-foreground"
      data-system-color-scheme={systemColorScheme}
    >
      <StartupWindowDragRegion />
      <OnboardingChrome current={5} />
      <OnboardingFooterProvider>
        <div className="relative flex min-h-0 w-full max-w-[920px] flex-1 flex-col items-center text-center">
          {page === "welcome" ? (
            <OnboardingSlideTransition
              className="flex h-full min-h-0 w-full flex-col items-center text-center"
              containerClassName="h-full min-h-0 [&>.buzz-onboarding-transition-line]:h-full"
              direction={transitionDirection}
              effect={welcomeEffect}
              transitionKey={`welcome-${welcomeEffect}-${transitionDirection}`}
            >
              <div className="w-full max-w-[760px]">
                <h1 className="text-title font-normal">
                  Join or create a community
                </h1>
                <p className="mt-3 text-sm leading-6 text-foreground/80">
                  Join with an invite, create your own community, or reconnect
                  one you already have.
                </p>
              </div>
              <div className="flex w-full flex-1 translate-y-16 flex-col items-center justify-center gap-20 py-8">
                <Card
                  asChild
                  className={COMMUNITY_OPTION_CARD_CLASS}
                  variant="textured"
                >
                  <button
                    data-testid="community-choice-join"
                    onClick={() => showPage("join")}
                    type="button"
                  >
                    Join a community
                  </button>
                </Card>
                <Card
                  asChild
                  className={COMMUNITY_OPTION_CARD_CLASS}
                  variant="textured"
                >
                  <button
                    data-testid="community-choice-nip17"
                    onClick={() => showPage("nip17")}
                    type="button"
                  >
                    Connect privately
                  </button>
                </Card>
                <Card
                  asChild
                  className={COMMUNITY_OPTION_CARD_CLASS}
                  variant="textured"
                >
                  <button
                    data-testid="community-choice-create"
                    onClick={() => setIsHostedSignInOpen(true)}
                    type="button"
                  >
                    Create a community
                  </button>
                </Card>
                <Card
                  asChild
                  className={COMMUNITY_OPTION_CARD_CLASS}
                  variant="textured"
                >
                  <button
                    data-testid="community-choice-existing"
                    onClick={() => showPage("existing")}
                    type="button"
                  >
                    I already have a community
                  </button>
                </Card>
              </div>
              <OnboardingFooter>
                <Button
                  className="h-9 rounded-full bg-foreground/10 px-6 hover:bg-foreground/15"
                  data-testid="welcome-setup-back"
                  onClick={onBack}
                  type="button"
                  variant="ghost"
                >
                  Back
                </Button>
              </OnboardingFooter>
            </OnboardingSlideTransition>
          ) : page === "existing" ? (
            <OnboardingSlideTransition
              className="flex h-full min-h-0 w-full flex-col items-center text-center"
              containerClassName="h-full min-h-0 [&>.buzz-onboarding-transition-line]:h-full"
              direction={transitionDirection}
              transitionKey={`existing-${transitionDirection}`}
            >
              <div className="w-full max-w-[760px]">
                <h1 className="text-title font-normal">
                  Reconnect to your community
                </h1>
                <p className="mt-3 text-sm leading-6 text-foreground/80">
                  Tell us your role so we can find the fastest way back in.
                </p>
              </div>
              <div className="flex w-full flex-1 translate-y-16 flex-col items-center justify-center gap-20 py-8">
                <Card
                  asChild
                  className={COMMUNITY_OPTION_CARD_CLASS}
                  variant="textured"
                >
                  <button
                    data-testid="existing-choice-owner"
                    onClick={() => setIsHostedSignInOpen(true)}
                    type="button"
                  >
                    I own the community
                  </button>
                </Card>
                <Card
                  asChild
                  className={COMMUNITY_OPTION_CARD_CLASS}
                  variant="textured"
                >
                  <button
                    data-testid="existing-choice-member"
                    onClick={() => showPage("member")}
                    type="button"
                  >
                    I’m a member or admin
                  </button>
                </Card>
              </div>
              <OnboardingFooter>
                <Button
                  className="h-9 rounded-full bg-foreground/10 px-6 hover:bg-foreground/15"
                  data-testid="existing-back"
                  onClick={() => showPage("welcome")}
                  type="button"
                  variant="ghost"
                >
                  Back
                </Button>
              </OnboardingFooter>
            </OnboardingSlideTransition>
          ) : page === "nip17" ? (
            <OnboardingSlideTransition
              className="flex w-full flex-col items-center text-center"
              direction={transitionDirection}
              transitionKey={`nip17-${transitionDirection}`}
            >
              <div className="w-full max-w-[620px]">
                <h1 className="text-title font-normal">Connect privately</h1>
                <p className="mt-3 text-sm leading-6 text-foreground/80">
                  Connect through a NIP-17 gateway on public Nostr relays. No
                  direct relay URL is required.
                </p>
              </div>
              <form
                className="mt-12 flex w-full max-w-[520px] flex-col gap-3 text-left"
                onSubmit={(event) => {
                  event.preventDefault();
                  startNip17Connection();
                }}
              >
                <Input
                  onChange={(event) => setNip17Name(event.target.value)}
                  placeholder="Community name"
                  value={nip17Name}
                />
                <Input
                  onChange={(event) => {
                    setNip17GatewayPubkey(event.target.value);
                    setNip17Error(null);
                  }}
                  placeholder="Gateway pubkey (64 hex characters)"
                  value={nip17GatewayPubkey}
                />
                <textarea
                  className="min-h-24 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                  onChange={(event) => {
                    setNip17PublicRelayUrls(event.target.value);
                    setNip17Error(null);
                  }}
                  placeholder={"wss://relay.example\nwss://relay2.example"}
                  value={nip17PublicRelayUrls}
                />
                {nip17Error ? (
                  <p className="text-sm text-destructive">{nip17Error}</p>
                ) : null}
                <Button className="w-full" type="submit">
                  Connect privately
                </Button>
              </form>
              <OnboardingFooter>
                <Button
                  className="h-9 rounded-full bg-foreground/10 px-6 hover:bg-foreground/15"
                  onClick={() => showPage("welcome")}
                  type="button"
                  variant="ghost"
                >
                  Back
                </Button>
              </OnboardingFooter>
            </OnboardingSlideTransition>
          ) : page === "owned" ? (
            <OnboardingSlideTransition
              className="flex w-full flex-col items-center text-center"
              direction={transitionDirection}
              transitionKey={`owned-${transitionDirection}`}
            >
              <HostedCommunityOnboarding onBack={() => showPage("welcome")} />
            </OnboardingSlideTransition>
          ) : (
            <OnboardingSlideTransition
              className="flex min-h-[calc(100dvh-15.625rem)] w-full flex-col items-center text-center"
              direction={transitionDirection}
              transitionKey={`${page}-${transitionDirection}`}
            >
              <div className="w-full max-w-[620px]">
                <h1 className="text-title font-normal">
                  {page === "member"
                    ? "Reconnect to your community"
                    : "Join a community"}
                </h1>
                <p className="mt-3 text-sm leading-6 text-foreground/80">
                  {page === "member"
                    ? "Enter the community URL or an invite link. Your role will be restored when you connect."
                    : "Enter the invite link or community URL you received."}
                </p>
              </div>
              <div className="flex w-full flex-1 flex-col items-center justify-center gap-16">
                <InviteRedeemForm
                  error={null}
                  isRedeeming={false}
                  onCancel={() =>
                    showPage(page === "member" ? "existing" : "welcome")
                  }
                  onConnect={startConnection}
                  onRedeem={redeemInvite}
                  placeholder="Invite link or community URL"
                  variant="onboarding-spotlight"
                />
                {page === "join" ? (
                  <div className="w-full max-w-[560px] text-left">
                    <p className="text-sm font-medium text-foreground">
                      Joining a private community?
                    </p>
                    <p className="mt-2 text-sm leading-6 text-foreground/75">
                      Some communities need the owner to add you before you can
                      join. Copy your public ID and send it to the community
                      owner.
                    </p>
                    <div className="mt-4 flex items-center gap-3 rounded-xl border border-foreground/10 bg-background/35 px-4 py-3">
                      <code
                        className="min-w-0 flex-1 truncate font-mono text-xs text-foreground/80"
                        data-testid="welcome-join-npub"
                      >
                        {npub || "Loading…"}
                      </code>
                      <Button
                        aria-label="Copy public ID"
                        className="h-9 shrink-0 rounded-full px-3"
                        disabled={!npub}
                        onClick={() => {
                          void writeTextToClipboard(npub).then(() => {
                            setCopiedNpub(true);
                            window.setTimeout(() => setCopiedNpub(false), 1500);
                          });
                        }}
                        size="sm"
                        type="button"
                        variant="outline"
                      >
                        {copiedNpub ? (
                          <Check className="h-4 w-4" aria-hidden="true" />
                        ) : (
                          <Copy className="h-4 w-4" aria-hidden="true" />
                        )}
                        <span>{copiedNpub ? "Copied" : "Copy"}</span>
                      </Button>
                    </div>
                    {npubError ? (
                      <p className="mt-3 text-sm text-destructive">
                        {npubError}
                      </p>
                    ) : null}
                  </div>
                ) : null}
              </div>
            </OnboardingSlideTransition>
          )}
          {isHostedSignInOpen && page !== "owned" ? (
            <HostedCommunityOnboarding
              onBack={() => setIsHostedSignInOpen(false)}
              onReady={() => {
                setIsHostedSignInOpen(false);
                showPage("owned");
              }}
              stageHidden
            />
          ) : null}
        </div>
      </OnboardingFooterProvider>
    </div>
  );
}
