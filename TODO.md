# Service identity and sharing roadmap

Extend the current HTTP(S)-over-iroh bridge with permissions for individual
services. Keep apps using ordinary local HTTP(S) addresses. Audit fixes are
being handled separately and are not tracked here.

## Next increment: local service identities and permissions

- [ ] Give each logical service a persistent service ID, separate from its
  hostname, display name, and hosting computer's iroh endpoint ID. Record the
  owning personal workspace or group.
- [ ] Give unrelated installations different service IDs. Treat multiple hosts
  as interchangeable only when explicitly configured as providers of the same
  logical service, with compatible application state.
- [ ] Persist local hostname bindings such as `notes.localhost → service-7a91`.
  Remote announcements must not overwrite an existing binding. Use a separate
  hostname for an unrelated service to preserve browser origin separation.
- [ ] Add an export registry owned by the parent application: service ID → local
  service target. Keep HEAD probing as an availability check for that target;
  a positive HEAD response alone must not grant hosting permission.
- [ ] Store an approved provider list and an allowed client list per service.
  Permission to access a service must be independent of permission to host it
  or administer its permissions.
- [ ] Define how existing hostname routes and paired devices migrate into this
  model. Make approval of initial service bindings explicit before forwarding
  requests containing app credentials to a newly selected provider.

## Discovery and forwarding

- [ ] Resolve the local hostname to its saved service ID before discovering a
  provider. Treat peer responses as candidates that still require permission
  checks.
- [ ] Race availability checks only among authorized providers of the requested
  service. The first available authorized provider wins.
- [ ] Verify the selected provider's authenticated iroh identity against its
  hosting permission before sending the app request, including cookies,
  authorization headers, or its body.
- [ ] On the receiving computer, verify the authenticated caller's permission
  to access that service before forwarding anything to the local target.
- [ ] Resolve the destination from the receiver's own service registry. Ensure
  changing the HTTP Host header cannot redirect an authorized service stream
  into a different local service.
- [ ] Keep the selected provider cached until it becomes unavailable, or its
  permission is revoked or expires. Keep route caching independent of the
  validity period of permissions.
- [ ] On provider failure, try other approved providers of the same service.
  Report unavailable when none qualify; do not let an unrelated installation
  take over the hostname. Preserve the existing rule against replaying writes.
- [ ] Share discovery information only with devices permitted to discover the
  relevant services; avoid broadcasting private service catalogs to all peers.

## Personal and shared workspace onboarding

- [ ] Add an **Add my computer** flow: enroll the device into the personal
  workspace and show which services it may access and host.
- [ ] Add a **Join a shared workspace** flow: accept membership, then expose only
  services granted to that membership. Membership alone must not authorize a
  device to host replacements for existing services.
- [ ] Show the service owner, approved providers, and sharing permissions in the
  launcher. Keep device and user names as display metadata; base permissions on
  authenticated identities and verified membership.
- [ ] Keep private and shared services on distinct local names where needed,
  such as `notes.localhost` and `notes.team.localhost`. Resolve naming conflicts
  through the launcher, without silently replacing existing browser origins.
- [ ] Explain the remaining trust boundary: an approved provider receives that
  service's traffic. Transport permissions do not replace the application's
  own authentication or protect its plaintext from a compromised approved host.

## Later: signed ownership, membership, and grants

- [ ] Define the trusted owner or group administrator identity. Start with a
  launcher-managed administrator key or integrate an account authority; keep
  every device's iroh identity independent.
- [ ] Define versioned, signed membership and service permission records with
  issuer, subject identity, workspace, service ID, rights, expiry, and revision.
- [ ] Bind grants to authenticated device identities, or to verified user/group
  membership associated with those devices, so copying a grant alone does not
  authorize an unrelated endpoint.
- [ ] Validate records against an already trusted owner or administrator. A
  provider's self-signed announcement must not be sufficient to authorize it
  to host somebody else's service.
- [ ] Support device removal, grant revocation, and key rotation. Persist the
  latest accepted policy revision and reject older replayed policy records.
- [ ] Choose an explicit offline policy. Specify how long cached grants remain
  usable and how revocations propagate; do not promise immediate revocation on
  disconnected devices. Enforce known revocations and grant expiry locally.
- [ ] Choose how signed policies are distributed: an application backend offers
  simpler central administration, while peer distribution supports offline use
  with explicit limits on policy staleness.
- [ ] Keep direct peer queries initially. Consider `iroh-gossip` for distributing
  signed announcements if the number of devices warrants it. Discovery and
  message distribution must remain separate from authorization decisions.

## Focused integration tests

- [ ] An unauthorized provider answering first receives no app request or
  credentials; an approved available provider is selected instead.
- [ ] A device allowed to access a service cannot claim permission to host it.
- [ ] A paired device without access to a service cannot reach its local target.
- [ ] Altering Host cannot escape the service ID authorized for a stream.
- [ ] Duplicate display names and hostname announcements cannot replace an
  existing local service binding.
- [ ] Provider failover stays within one service ID and its approved hosts;
  unrelated installations remain unavailable through that alias.
- [ ] Route caches survive restarts but are invalidated by relevant permission
  changes. Cached routes do not bypass expired or revoked grants.
- [ ] Group membership, hosting rights, and client access are enforced
  independently, including removal, offline expiry, and policy replay cases.

## Building blocks

- [Iroh](https://docs.rs/iroh/) supplies authenticated device connections,
  encrypted transport, and endpoint address lookup.
- [iroh-gossip](https://docs.rs/iroh-gossip/) can distribute announcements among
  peers. Service ownership and permission validation belong to this application.

Start with stable service IDs and local allowlists. Signed group policies are a
later increment, not a prerequisite for the personal-device starter.
