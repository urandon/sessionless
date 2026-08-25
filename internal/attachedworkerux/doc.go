// Package attachedworkerux reduces the owner-scoped AW-01 through AW-05
// authorities into the versioned, public-safe AW-06 read model. Reads are
// observation-only: the package does not refresh presence, evaluate admission,
// advance protocol state, or execute control actions.
package attachedworkerux
