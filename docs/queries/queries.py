from collections import deque
from dataclasses import dataclass
import enum
from typing import Callable, Deque, Generator, Optional, TypeVar


VertexPropsT = TypeVar("VertexPropsT")
VertexHashIndex = dict["VertexID", "BaseVertex"]

SurogateID = str
EdgeID = SurogateID
VertexID = SurogateID
EdgeBlockID = SurogateID


# =====================================================
#               Vertex Tables
# =====================================================


@dataclass
class BaseVertex:
    id: VertexID
    edge_block_id: EdgeBlockID


@dataclass
class SomeVertex(BaseVertex): ...


@dataclass
class OtherVertex(BaseVertex): ...


class VertexTypeTag(enum.IntEnum):
    SomeVertexTag = 0
    OtherVertexTag = 1
    ...


VERTEX_TABLE_RESOLVER = {
    VertexTypeTag.SomeVertexTag: VertexHashIndex({}),
    VertexTypeTag.OtherVertexTag: VertexHashIndex({}),
    # ...
}


# =====================================================
#               Vertex Tables [END]
# =====================================================


# =====================================================
#               Edge Tables
# =====================================================


EdgePropsT = TypeVar("EdgePropsT")
IncidentVertexPropsT = TypeVar("IncidentVertexPropsT")


@dataclass
class BaseEdge:
    id: EdgeID
    next_id: Optional[EdgeID]
    incident_v_id: VertexID


@dataclass
class SomeEdge(BaseEdge): ...


@dataclass
class OtherEdge(BaseEdge): ...


EdgeHashIndex = dict[EdgeID, BaseEdge]


class EdgeTypeTag(enum.IntEnum):
    SomeEdgeTag = 0
    OtherEdgeTag = 1
    ...


EDGE_TABLE_RESOLVER = {
    EdgeTypeTag.SomeEdgeTag: EdgeHashIndex({}),
    EdgeTypeTag.OtherEdgeTag: EdgeHashIndex({}),
    # ...
}

# =====================================================
#               Edge Tables [END]
# =====================================================


@dataclass
class DirectoryEntry:
    edge_type_tag: EdgeTypeTag
    edge_id: Optional[EdgeID]
    next_block_id: Optional[EdgeBlockID]


DirectoryHashIndex = dict[EdgeBlockID, DirectoryEntry]
TypedDirectoryHashIndex = dict[tuple[VertexID, EdgeTypeTag], DirectoryEntry]


DIRECTORY_INDEX_RESOLVER: dict[VertexTypeTag, DirectoryHashIndex] = {
    VertexTypeTag.SomeVertexTag: DirectoryHashIndex({}),
    VertexTypeTag.OtherVertexTag: DirectoryHashIndex({}),
}

TYPED_DIRECTORY_INDEX_RESOLVER: dict[VertexTypeTag, TypedDirectoryHashIndex] = {
    VertexTypeTag.SomeVertexTag: TypedDirectoryHashIndex({}),
    VertexTypeTag.OtherVertexTag: TypedDirectoryHashIndex({}),
}

EDGE_CHECKER = {
    VertexTypeTag.SomeVertexTag: [
        EdgeTypeTag.SomeEdgeTag,
        EdgeTypeTag.OtherEdgeTag,
    ],
    VertexTypeTag.OtherVertexTag: [],
}


# тип ребра назначения также равен Vertex. 
# Будет механизм уведомления о нарушении
def assert_edge_T_T(v_tag: VertexTypeTag, e_tag: EdgeTypeTag):
    pass


def vertex_edges(
    v_tag: VertexTypeTag, v_id: VertexID, e_tag: EdgeTypeTag
) -> Generator[BaseEdge]:
    d = TYPED_DIRECTORY_INDEX_RESOLVER[v_tag]
    edge_table = EDGE_TABLE_RESOLVER[e_tag]
    block = d[(v_id, e_tag)]
    next_edge_id = block.edge_id
    while next_edge_id is not None:
        edge = edge_table[next_edge_id]
        yield edge
        next_edge_id = edge.next_id


class Traverser:
    def __init__(
        self,
        vertex_tag: VertexTypeTag,
        vertex_filter: Callable[[BaseVertex], bool],
        edge_tag: EdgeTypeTag,
        edge_filter: Callable[[BaseEdge], bool],
        max_depth: int,
    ):
        self._seen_vs: set[VertexID] = set()
        self.v_tag = vertex_tag
        self.vertex_filter = vertex_filter
        self.e_tag = edge_tag
        self.edge_filter = edge_filter
        self.max_depth = max_depth
        self._result: list[VertexID] = []
        self.v_table = VERTEX_TABLE_RESOLVER[self.v_tag]
        assert_edge_T_T(self.v_tag, self.e_tag)

    @staticmethod
    def DFS(
        vertex_id: VertexID,
        vertex_tag: VertexTypeTag,
        vertex_filter: Callable[[BaseVertex], bool],
        edge_tag: EdgeTypeTag,
        edge_filter: Callable[[BaseEdge], bool],
        max_depth: int,
    ) -> list[VertexID]:
        t = Traverser(vertex_tag, vertex_filter, edge_tag, edge_filter, max_depth)

        return t.dfs_traverse(vertex_id)

    @staticmethod
    def BFS(
        vertex_id: VertexID,
        vertex_tag: VertexTypeTag,
        vertex_filter: Callable[[BaseVertex], bool],
        edge_tag: EdgeTypeTag,
        edge_filter: Callable[[BaseEdge], bool],
        max_depth: int,
    ) -> list[VertexID]:
        t = Traverser(vertex_tag, vertex_filter, edge_tag, edge_filter, max_depth)

        return t.bfs_traverse(vertex_id)

    def dfs_traverse(self, vertex_id: VertexID) -> list[VertexID]:
        self._seen_vs.clear()
        self._result.clear()
        self._dfs_traverse_helper(vertex_id, 1)
        return self._result

    def bfs_traverse(self, v_id: VertexID) -> list[VertexID]:
        self._seen_vs.clear()
        self._result.clear()

        d: Deque[tuple[VertexID, int]] = deque()
        d.append((v_id, 0))
        while len(d) > 0:
            (v_id, depth) = d.popleft()
            next_depth = depth + 1
            edges: Generator[BaseEdge] = vertex_edges(self.v_tag, v_id, self.e_tag)
            for edge in filter(self.edge_filter, edges):
                v = self.v_table[edge.incident_v_id]
                if not self.vertex_filter(v):
                    continue
                if next_depth == self.max_depth:
                    self._result.append(edge.id)
                elif v_id not in self._seen_vs:
                    d.append((v_id, next_depth))
                    self._seen_vs.add(v_id)
        return self._result

    def _dfs_traverse_helper(self, v_id: VertexID, cur_depth: int):
        if cur_depth == self.max_depth:
            self._result.append(v_id)
            return

        if v_id in self._seen_vs:
            return

        self._seen_vs.add(v_id)
        v = self.v_table[v_id]
        if not self.vertex_filter(v):
            return

        edges = vertex_edges(self.v_tag, v_id, self.e_tag)
        for edge in filter(self.edge_filter, edges):
            if not self.edge_filter(edge):
                continue
            self._dfs_traverse_helper(edge.incident_v_id, cur_depth + 1)


T = TypeVar("T")
TreeIndex = dict[T, list[BaseVertex]]


# запрос 2/3
# O(log V) - сложность спуска по индексу до нужного элема
# пусть m - число элемов со значением val,
# тогда O(log V + m * MaxDegree(G))
def select_where[T](
    v_tag: VertexTypeTag,
    index: TreeIndex[T],
    val: T,
    e_tag: EdgeTypeTag,
    edge_filter: Callable[[BaseEdge], bool],
    cutoff_degree: int,
) -> list[str]:
    res: list[VertexID] = []
    vv = index[val]
    for v in vv:
        edges = vertex_edges(v_tag, v.id, e_tag)
        count = sum(1 for _ in filter(edge_filter, edges))
        if cutoff_degree > count:
            continue
        res.append(v.id)
    return res


# запрос 4
# O(V + E)
def aggregate(
    v_tag: VertexTypeTag,
    e_tag: EdgeTypeTag,
    edge_filter: Callable[[BaseEdge], bool],
    param_extractor: Callable[[BaseVertex], float],
    pred: Callable[[float], bool],
) -> dict[VertexID, float]:
    info: dict[VertexID, float] = {}
    v_table = VERTEX_TABLE_RESOLVER[v_tag]
    for v in v_table.values():
        edges = vertex_edges(v_tag, v.id, e_tag)
        s = 0
        for edge in filter(edge_filter, edges):
            n = v_table[edge.incident_v_id]
            param = param_extractor(n)
            s += param
        if pred(s):
            info[v.id] = s
    return info


# запрос 5
# O(V * E)
# https://docs.tigergraph.com/graph-ml/3.10/community-algorithms/triangle-counting#_specifications
def count_triangles(v_tag: VertexTypeTag, e_tag: EdgeTypeTag) -> float:
    assert_edge_T_T(v_tag, e_tag)
    vertex_table = VERTEX_TABLE_RESOLVER[v_tag]

    c = 0
    for v_id in vertex_table:
        left_neighbourhood = set(e.id for e in vertex_edges(v_tag, v_id, e_tag))
        for edge in vertex_edges(v_tag, v_id, e_tag):
            right_neighbourhood = set(
                e.id for e in vertex_edges(v_tag, edge.incident_v_id, e_tag)
            )

            c += len(left_neighbourhood.intersection(right_neighbourhood))

    return c / 3
