import * as d3 from 'd3';

export const colors = ['#EFE0DB', '#F5D66E', '#F1A21A', '#E6653E'];

export const colorScale = d3.scaleOrdinal()
    .domain([0.7, 0.8, 0.9, 1.0])
    .range(colors)