.. _core:

:c:struct:`raft` --- Core engine
================================

The :c:struct:`raft` struct is the central part of C-Raft. It holds and drive
the state of a single Raft server in a cluster.

It is purely a finite state machine, and it doesn't perform any I/O or system
calls.

The :c:func:`raft_step()` function is used to advance the state of a
:c:struct:`raft` state machine, and is designed to be integrated in some
external event loop or I/O layer that is in charge of receiving users requests,
implementing network communication with other Raft servers, persisting data to
disk.

For example:

.. code-block:: C

   /* A RequestVote RPC message has been received from the network. We inform
    * struct raft about that by passing a struct raft_event to raft_step(). */
   struct raft raft;
   struct raft_event event;
   struct raft_update update;

   event.type = RAFT_RECEIVE;
   event.receive.message = ...; /* Fill with the content of the message */

   raft_step(&raft, &event, &update);

   /* The struct raft_update object contains information about the next actions
    * the I/O layer should perform, for example it might contain new messages to
    * be sent. */
   if (update.flags & RAFT_UPDATE_MESSAGES) {
       for (unsigned i = 0; i < update.messages.n; i++) {
           /* Send the message contained in update.messages.batch[i] */
       }
   }

Basically whenever an event occurs in the I/O layer, the :c:func:`raft_step()`
function must be called and the resulting state updates should be performed.

See the `External events`_ section for details about what events to pass to the
step function in order to drive the state machine forward, and `State updates`_
for details about how state updates should be processed after calling the step
function.

.. _External events: ./events.html
.. _State updates: ./updates.html

Data types
----------

.. c:struct:: raft

    A single raft server in a cluster.

.. c:type:: raft_id

   Hold the value of a raft server ID. Guaranteed to be at least 64-bit long.


Public members
^^^^^^^^^^^^^^

.. c:member:: raft_id raft.id

    Server ID. Readonly.

API
---

.. c:function:: int raft_init(struct raft *r, raft_id id, const char *address)

    Initialize a raft state machine.

.. c:function:: int raft_close(struct raft* r)

    Close a raft state machine, releasing all memory it uses.

.. c:function:: int raft_step(struct raft* r, struct raft_event *event, struct raft_update *update)

   Advance the state of the given raft state machine.

