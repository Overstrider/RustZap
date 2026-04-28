"use client";

import { useMemo } from "react";
import { useRustZap } from "@/components/rustzap-provider";

export default function GroupsPage() {
  const { state, actions } = useRustZap();
  const selectedGroup = useMemo(
    () => state.groups.find((group) => group.id === state.selectedGroupId) ?? state.groups[0],
    [state.groups, state.selectedGroupId]
  );
  const admins = selectedGroup?.members.filter((member) => member.isAdmin) ?? [];
  const members = selectedGroup?.members ?? [];

  return (
    <section className="page">
      <div className="page-header">
        <div>
          <h1>Groups</h1>
          <p>Inspect group metadata, members, admin status, and simulated group events.</p>
        </div>
        {selectedGroup ? (
          <div className="toolbar">
            <button type="button" onClick={() => actions.simulateGroupEvent(selectedGroup.id, "join")}>
              Sim Join
            </button>
            <button type="button" onClick={() => actions.simulateGroupEvent(selectedGroup.id, "leave")}>
              Sim Leave
            </button>
            <button type="button" disabled={!selectedGroup.canExit} onClick={() => actions.exitGroup(selectedGroup.id)}>
              Exit
            </button>
          </div>
        ) : null}
      </div>

      <div className="workspace-grid">
        <aside className="surface list-surface">
          <h2>Group List</h2>
          <div className="compact-list">
            {state.groups.map((group) => (
              <button
                className={group.id === selectedGroup?.id ? "active" : undefined}
                key={group.id}
                type="button"
                onClick={() => actions.selectGroup(group.id)}
              >
                <span>
                  <b>{group.subject}</b>
                  <small>{group.members.length} members · {group.role}</small>
                </span>
              </button>
            ))}
          </div>
        </aside>

        <section className="surface">
          {selectedGroup ? (
            <>
              <div className="section-header">
                <div>
                  <h2>{selectedGroup.subject}</h2>
                  <p>{selectedGroup.id}</p>
                </div>
                <span className={selectedGroup.canManage ? "capability enabled" : "capability disabled"}>
                  {selectedGroup.canManage ? "admin controls" : "read only"}
                </span>
              </div>
              <dl className="details-list">
                <div>
                  <dt>Description</dt>
                  <dd>{selectedGroup.description}</dd>
                </div>
                <div>
                  <dt>Owner</dt>
                  <dd>{selectedGroup.ownerJid}</dd>
                </div>
                <div>
                  <dt>Admins</dt>
                  <dd>{admins.length}</dd>
                </div>
                <div>
                  <dt>Status</dt>
                  <dd>{selectedGroup.exitedAt ? `exited at ${selectedGroup.exitedAt}` : "active"}</dd>
                </div>
              </dl>

              <h2>Members</h2>
              <div className="table-list">
                {members.map((member) => (
                  <div key={member.contactId}>
                    <span>
                      <b>{member.name}</b>
                      <small>{member.phoneE164}</small>
                    </span>
                    <span>{member.role}</span>
                    <span>{member.isAdmin ? "admin" : "member"}</span>
                  </div>
                ))}
              </div>
            </>
          ) : (
            <p className="empty-state">No groups available.</p>
          )}
        </section>
      </div>
    </section>
  );
}
