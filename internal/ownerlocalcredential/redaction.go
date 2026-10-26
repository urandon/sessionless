package ownerlocalcredential

// These objects retain private keys, references and exact invocation handles.
// Generic formatting must not traverse their private fields; serialization is
// denied rather than inventing a public snapshot DTO.
func (Store) String() string                   { return "ownerlocalcredential.Store([redacted])" }
func (Store) GoString() string                 { return "ownerlocalcredential.Store([redacted])" }
func (Store) MarshalJSON() ([]byte, error)     { return nil, ErrDenied }
func (Lifecycle) String() string               { return "ownerlocalcredential.Lifecycle([redacted])" }
func (Lifecycle) GoString() string             { return "ownerlocalcredential.Lifecycle([redacted])" }
func (Lifecycle) MarshalJSON() ([]byte, error) { return nil, ErrDenied }
