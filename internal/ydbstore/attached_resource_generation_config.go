package ydbstore

// AttachedResourceGenerationEnabledV1 only controls registration of the
// metadata HTTP routes. It performs no reads and grants no execution authority;
// every operation still checks the exact installed baseline in its transaction.
func (store *Store) AttachedResourceGenerationEnabledV1() bool {
	return store != nil && store.attachedResourceGenerations.Len() > 0
}
