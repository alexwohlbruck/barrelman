"""segments — stage-6 steady/transition segmentation of the ordered transit graph.

Input:  transit_graph_nodes/edges/edge_lines for a build_key whose slots
        passed the stage-5 stability exam (corridor-stable global optimum).
Output: transit_line_segments — per-ribbon display features: long steady
        corridor pieces with a constant screen-px offset, and short
        fixed-ground-length transition pieces straddling junction nodes
        with off_from_px/off_to_px for the MapLibre variable line-offset
        fork (docs/transit-pipeline-v3.md stage 6, PAR-12 v3 contract).
"""
