# Release checklist (nothing is published by this example)

- Track the change in an issue and assign plugin SemVer independently of host
  and SDK versions. A changed event meaning requires a new contract/capability,
  not a reinterpretation of versioned data.
- Review configuration fields, exact instrument mapping, bounded synchronous
  lifecycle, sequence/clock semantics and honest optional data.
- Review dependency licenses and supply-chain provenance separately from RECORD.
- Keep credentials/provider data out; approve any raw provenance/hash before
  constructing it. Update both resource permissions and independent provider
  rights requirements, never use a capability as authorization.
- Regenerate metadata and driver; `--check` must pass. Verify wheel/sdist include
  source, canonical declarations and `py.typed`; install rebuilt sdist wheel too.
- In clean minimum/current Python environments install normal dependencies,
  run `pip check`, all project tests and type checks. Audit SDK-only imports.
- Run every required public conformance case against the actual installed
  candidate. Preserve failures and complete denominators; no skips as success.
- Use public authorized host APIs to capture/replay canonical generated events,
  verify health/provenance and original expected roots, then prove independent
  uninstall removes only the plugin. Do not activate real feeds for this gate.
- Record exact host wheel, plugin wheel, SDK, driver, configuration and report
  identities, tested OS/isolation/profile/deadlines and known limits.
- Confirm conformance is not provider/scientific qualification. A real provider
  integration remains independently gated; generated data cannot close it.
- Before any host release, follow its dev/main, Commitizen, full hooks/tests,
  local-simple-registry TestPyPI preflight and TestPyPI-before-PyPI policy.
  This template creates no release, tag, publication or version bump itself.

