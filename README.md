# The concept
The idea is that we'll have different Actor structs impleemnted in golang packages.
Each actor will need to identify itself via a unique `type` string so that
when a worker fetches the data for the actor it's able to construct the right
type of struct so that the `processMessages()` will do what we expect. Each Actor type 
would implement a `processMessages()` which returns back an appropriately modified `state` -- a map[string]interface{} -- to get persisted for the actor.

We have an ActorBase type because all Actor types need to have an ID, Inbox, and State.

We have an Actor interface which says that all Actors must implement a `processMessages()` method.

The `ActorRepositoryDdb.get()` method returns an `Actor` (interface) type.
Inside this method, it looks at the `type` attribute in Dynamo uses that info to construct and return the appropriate Actor struct (e.g. `CountingActor`) (see `repositories.go:199`)

Am I doing anything non-idiomatic with this? Is there a better way to have a sort of 'ABC'?