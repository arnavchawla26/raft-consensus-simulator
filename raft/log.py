"""The replicated log: a 1-indexed, append/truncate sequence of LogEntry.

Index 0 is a sentinel meaning "before the first entry" (term 0), which
lets prev_log_index == 0 in AppendEntries be handled without a special
case anywhere else in the codebase.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional


@dataclass(frozen=True)
class LogEntry:
    term: int
    index: int
    command: Any


class RaftLog:
    def __init__(self) -> None:
        self._entries: List[LogEntry] = []

    def __len__(self) -> int:
        return len(self._entries)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RaftLog):
            return NotImplemented
        return self._entries == other._entries

    def __repr__(self) -> str:
        return f"RaftLog({self._entries!r})"

    @property
    def last_index(self) -> int:
        return len(self._entries)

    def last_term(self) -> int:
        return self._entries[-1].term if self._entries else 0

    def term_at(self, index: int) -> int:
        """Term of the entry at `index`, or 0 for index 0 or out-of-range."""
        if index <= 0 or index > len(self._entries):
            return 0
        return self._entries[index - 1].term

    def entry_at(self, index: int) -> Optional[LogEntry]:
        if index < 1 or index > len(self._entries):
            return None
        return self._entries[index - 1]

    def entries_from(self, index: int) -> List[LogEntry]:
        """All entries at position >= index (1-based). Empty if index > last_index."""
        if index < 1:
            index = 1
        return list(self._entries[index - 1:])

    def all_entries(self) -> List[LogEntry]:
        return list(self._entries)

    def append(self, term: int, command: Any) -> LogEntry:
        """Append a brand-new entry (used by the leader for client commands)."""
        entry = LogEntry(term=term, index=len(self._entries) + 1, command=command)
        self._entries.append(entry)
        return entry

    def append_entries(self, entries: List[LogEntry]) -> None:
        """Append a batch of already-indexed entries (used by followers)."""
        self._entries.extend(entries)

    def truncate_from(self, index: int) -> None:
        """Delete the entry at `index` (1-based) and everything after it."""
        if index < 1:
            self._entries = []
        else:
            self._entries = self._entries[: index - 1]

    def candidate_log_is_up_to_date(self, candidate_last_term: int, candidate_last_index: int) -> bool:
        """Raft 5.4.1: is a candidate whose log ends at (term, index) at least
        as up-to-date as *this* (the voter's) log?
        """
        my_last_term = self.last_term()
        my_last_index = self.last_index
        if candidate_last_term != my_last_term:
            return candidate_last_term > my_last_term
        return candidate_last_index >= my_last_index
