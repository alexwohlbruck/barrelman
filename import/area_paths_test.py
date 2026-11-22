import math
import unittest

from shapely import LineString, Polygon, box

from area_paths import corners, lines


def length(path):
    return sum(math.dist(a, b) for a, b in zip(path, path[1:]))


def shortest(edges, start, end):
    graph = {}
    for a, b in edges:
        graph.setdefault(a, []).append(b)
        graph.setdefault(b, []).append(a)
    best, todo = {start: 0.0}, [start]
    while todo:
        todo.sort(key=lambda p: best[p])
        here = todo.pop(0)
        for there in graph.get(here, []):
            d = best[here] + math.dist(here, there)
            if d < best.get(there, math.inf):
                best[there] = d
                todo.append(there)
    return best.get(end, math.inf)


class CornersTest(unittest.TestCase):
    def test_a_convex_plaza_has_none(self):
        self.assertEqual(corners(box(0, 0, 50, 30)), [])

    def test_an_l_shape_has_its_inner_corner(self):
        l_shape = Polygon([(0, 0), (40, 0), (40, 10), (10, 10), (10, 40), (0, 40)])
        self.assertEqual([c.xy for c in corners(l_shape)], [(10.0, 10.0)])

    def test_every_corner_of_a_hole(self):
        plaza = Polygon(box(0, 0, 60, 60).exterior, [box(20, 20, 40, 40).exterior.coords])
        self.assertEqual(len(corners(plaza)), 4)

    def test_a_slight_kink_is_not_a_corner(self):
        kinked = Polygon([(0, 0), (20, 0), (40, 0.5), (60, 0), (60, 30), (0, 30)])
        self.assertEqual(corners(kinked), [])


class LinesTest(unittest.TestCase):
    def test_crosses_an_open_plaza_straight(self):
        found = lines(box(0, 0, 50, 30), [(0, 0), (50, 30)])
        self.assertEqual(found, [((0, 0), (50, 30))])

    def test_bends_once_around_an_l_shape(self):
        l_shape = Polygon([(0, 0), (40, 0), (40, 10), (10, 10), (10, 40), (0, 40)])
        found = lines(l_shape, [(40, 5), (5, 40)])
        self.assertNotIn(((40, 5), (5, 40)), found)
        hugging = math.dist((40, 5), (10, 10)) + math.dist((10, 10), (5, 40))
        self.assertAlmostEqual(shortest(found, (40, 5), (5, 40)), hugging, delta=3.0)

    def test_goes_around_a_hole(self):
        plaza = Polygon(box(0, 0, 60, 60).exterior, [box(20, 20, 40, 40).exterior.coords])
        found = lines(plaza, [(0, 30), (60, 30)])
        self.assertNotIn(((0, 30), (60, 30)), found)
        around = math.dist((0, 30), (20, 40)) + 20 + math.dist((40, 40), (60, 30))
        self.assertAlmostEqual(shortest(found, (0, 30), (60, 30)), around, delta=4.0)

    def test_keeps_off_the_corner_it_turns(self):
        l_shape = Polygon([(0, 0), (40, 0), (40, 10), (10, 10), (10, 40), (0, 40)])
        bends = {xy for line in lines(l_shape, [(40, 5), (5, 40)]) for xy in line} - {(40, 5), (5, 40)}
        self.assertTrue(bends)
        self.assertTrue(all(8 < x < 9 and 8 < y < 9 for x, y in bends))

    def test_keeps_off_an_edge_it_walks_past(self):
        plaza = Polygon(box(0, 0, 60, 40).exterior, [box(20, 0.5, 40, 20.2).exterior.coords])
        found = lines(plaza, [(0, 21), (60, 21)])
        self.assertNotIn(((0, 21), (60, 21)), found)
        hole = box(20, 0.5, 40, 20.2)
        for a, b in found:
            if a[0] < 40 and b[0] > 20:
                self.assertGreater(LineString([a, b]).distance(hole), 1.4)

    def test_still_threads_a_narrow_passage(self):
        rooms = box(0, 0, 20, 20).union(box(40, 0, 60, 20)).union(box(18, 15, 42, 17))
        found = lines(rooms, [(0, 5), (60, 5)])
        self.assertLess(shortest(found, (0, 5), (60, 5)), math.inf)

    def test_walks_past_a_planter(self):
        plaza = Polygon(box(0, 0, 60, 60).exterior, [box(29, 29, 31, 31).exterior.coords])
        self.assertEqual(lines(plaza, [(0, 30), (60, 30)]), [((0, 30), (60, 30))])

    def test_does_not_walk_through_a_wall(self):
        wall = LineString([(25, 0), (25, 20)])
        found = lines(box(0, 0, 50, 30), [(0, 5), (50, 5), (25, 20)], [wall])
        self.assertNotIn(((0, 5), (50, 5)), found)
        self.assertLess(shortest(found, (0, 5), (50, 5)), math.inf)

    def test_skips_a_line_through_another_point(self):
        found = lines(box(0, 0, 50, 30), [(0, 15), (25, 15), (50, 15)])
        self.assertNotIn(((0, 15), (50, 15)), found)

    def test_drops_lines_leaving_a_corner_backwards(self):
        l_shape = Polygon([(0, 0), (40, 0), (40, 10), (10, 10), (10, 40), (0, 40)])
        found = lines(l_shape, [(0, 0)])
        self.assertEqual(found, [])

    def test_gives_up_past_the_point_limit(self):
        self.assertIsNone(lines(box(0, 0, 50, 30), [(i, 0) for i in range(10)], max_points=5))


class PathLengthTest(unittest.TestCase):
    def test_never_longer_than_the_outline(self):
        plaza = Polygon([(0, 0), (30, -5), (60, 0), (55, 30), (30, 25), (5, 35)])
        found = lines(plaza, [(0, 0), (55, 30)])
        outline = length([(0, 0), (30, -5), (60, 0), (55, 30)])
        self.assertLessEqual(shortest(found, (0, 0), (55, 30)), outline)
        self.assertAlmostEqual(shortest(found, (0, 0), (55, 30)), math.dist((0, 0), (55, 30)))


if __name__ == "__main__":
    unittest.main()
