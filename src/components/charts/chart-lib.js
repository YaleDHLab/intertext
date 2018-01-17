import * as d3 from 'd3'

let ChartLib = {};

const curateProps = (_props) => {
  let props = Object.assign({}, _props)
  props.margin = props.margin || {top: 0, right: 0, bottom: 0, left: 0}
  props.height = props.height || 400;
  props.width = props.width || 400;
  return props;
}

/**
* Create chart foundation
**/

ChartLib.createBase = (elem, props) => {
  let viewportElem = null;
  const svg = d3.select(elem).append('svg');

  if (props.xLabel) svg.append('text').attr('class', 'x label')
  if (props.yLabel) svg.append('text').attr('class', 'y label')

  // Group container for svg children
  const g = svg.append('g').attr('class', 'group-container')
  
  // Grid backgrounds
  if (props.drawGrid) {
    g.append('rect').attr('class', 'grid');
    g.append('g').attr('class', 'x grid');
    g.append('g').attr('class', 'y grid');
  }

  // Brush viewport
  if (props.onBrush) {
    viewportElem = g.append('g').attr('class', 'viewport');
    viewportElem.append('rect');
  }

  // Specific geom containers
  if (props.areaData) g.append('g').attr('class', 'area-container')
  if (props.boxplotData) g.append('g').attr('class', 'boxplot-container')
  if (props.histogramData) g.append('g').attr('class', 'histogram-container')
  if (props.pointData) g.append('g').attr('class', 'point-container')
  if (props.lineData) g.append('g').attr('class', 'line-container')
  if (props.waffleData) g.append('g').attr('class', 'waffle-container')
  if (props.forceData) g.append('g').attr('class', 'force-container')
  if (props.swarmData) g.append('g').attr('class', 'swarm-container')

  // Legends
  if (props.legend) g.append('g').attr('class', 'legend-container')

  // Axes
  g.append('g').attr('class', 'x axis')
  g.append('g').attr('class', 'y axis')

  return viewportElem;
};

/**
* Update chart base with data
**/

ChartLib.updateBase = (elem, _props, viewportElem) => {
  const props = curateProps(_props)
  const svg = d3.select(elem).select('svg')
  svg.transition().duration(1000)
  setSvgSize(svg, props)
  transformGroupContainer(svg, props)
  transformXAxis(svg, props)
  if (props.drawGrid) addGrid(props)
  if (viewportElem) addViewport(viewportElem, props)
  if (props.xLabel) addXLabel(svg, props)
  if (props.yLabel) addYLabel(svg, props)
}

const setSvgSize = (svg, props) => {
  if (props.resize) {
    svg.attr('viewBox', '0 0 ' + props.width + ' ' + props.height)
      .attr('preserveAspectRatio', 'xMidYMid meet')
  } else {
    svg.attr('width', props.width).attr('height', props.height)
  }
}

const transformGroupContainer = (svg, props) => {
  svg.select('.group-container').attr('transform',
    'translate(' + props.margin.left + ',' + props.margin.top + ')')
}

const transformXAxis = (svg, props) => {
  svg.select('.x.axis')
    .attr('transform', 'translate(0,' + (props.height - props.margin.top - 
        props.margin.bottom) + ')');
}

const addGrid = (props) => {
  d3.select('.group-container').select('rect.grid')
    .attr('class', 'grid')
    .attr('width', props.width - props.margin.left - props.margin.right)
    .attr('height', props.height - props.margin.top - props.margin.bottom);
}

const addViewport = (viewportElem, props) => {
  viewportElem.select('.viewport rect')
    .attr('height', props.height - props.margin.top - props.margin.bottom)
    .attr('width', props.width - props.margin.left - props.margin.right)
    .attr('fill', 'none')
    .attr('stroke','grey')
}

const addXLabel = (svg, props) => {
  svg.select('.x.label')
    .attr('text-anchor', 'middle')
    .attr('y', props.height - props.margin.bottom + 30)
    .attr('x', props.width/2)
    .attr('dy', '.75em')
    .text(props.xLabel)
}

const addYLabel = (svg, props) => {
  svg.select('.y.label')
    .attr('text-anchor', 'middle')
    .attr('y', 1)
    .attr('x', 0 - ( (props.height - props.margin.bottom) / 2) )
    .attr('dy', '.75em')
    .attr('transform', 'rotate(-90)')
    .text(props.yLabel)
}

/**
* Get the geoms to be drawn
**/

ChartLib.getGeoms = (props) => {
  let geoms = [];

  if (props.pointData) {
    geoms.push({
      data: props.pointData,
      draw: ChartLib.drawPoint
    });
  }

  if (props.waffleData) {
    geoms.push({
      data: props.waffleData,
      draw: ChartLib.drawWaffle
    });
  }

  return geoms;
}

/**
* Get the domain for data to be plotted
**/

ChartLib.getDomain = (data, props) => {
  let domain = {};
  domain.x = d3.extent(data, (d) => d[props.x || 'x'])
  domain.y = d3.extent(data, (d) => d[props.y || 'y'])
  if (props.xDomain) domain.x = props.xDomain;
  if (props.yDomain) domain.y = props.yDomain;
  return domain;
}

/**
* Get the scales for a chart
**/

ChartLib.getScales = (elem, _props, domain) => {
  if (!domain) return null;
  let scales = {};
  const props = curateProps(_props)
  const margins = {top: 0, right: 80, bottom: 90, left: 0};
  const w = props.width - props.margin.right - props.margin.left;
  const h = props.height - props.margin.top - props.margin.bottom;
  const axes = {x: [0, w], y: [h, 0]};
  Object.keys(axes).map((a) => {
    if (props[a + 'Scale']) {
      switch (props[a + 'Scale']) {
        case 'ordinal':
          scales[a] = d3.scaleBand()
            .domain(domain[a])
            .range(axes[a])
          return;

        case 'linear':
          scales[a] = d3.scaleLinear()
            .domain(domain[a])
            .range(axes[a])
          return;

        case 'inverse':
          scales[a] = d3.scaleLinear()
            .domain(domain[a])
            .range([ axes[a][1], axes[a][0] ])
          return;

        default:
          console.warn('the requested', a, 'scale is invalid')
      }
    } else {
      scales[a] = d3.scaleLinear()
        .domain(domain[a])
        .range(axes[a])
    }
  })
  return scales;
}

/**
* Add axes
**/

ChartLib.updateAxes = (elem, props, scales, viewport, viewportElem) => {
  if (!scales) return;
  if (props.drawGrid == true) ChartLib.drawGrid(elem, props, scales)
  const axes = ChartLib.getAxes(props, scales)

  d3.select(elem).selectAll('g.x.axis')
    .call(axes.x)
    .selectAll('text')
      .attr('y', () => props.xLabelRotate ? 11 : 0 )
      .attr('x', () => props.xLabelRotate ? 1 : 0 )
      .attr('dy', () => props.xLabelRotate ? '.35em' : 0 )
      .style('text-anchor', () => props.xLabelRotate ? 'start' : 'middle')
      .attr('transform', () => props.xLabelRotate ?
          'rotate(' + props.xLabelRotate + ')'
        : 'rotate(0)')

  d3.select(elem).selectAll('g.y.axis')
    .transition()
    .duration(1000)
    .call(axes.y);

  if (props.onBrush && viewport && viewportElem) {
    viewportElem.call(viewport)
      .selectAll('rect')
      .attr('height', props.height - props.margin.top - props.margin.bottom);
  }
}

/**
* Get axes
**/

ChartLib.getAxes = (props, scales) => {
  return {
    x: d3.axisBottom(scales.x)
      .ticks(props.xTicks)
      .tickPadding(props.xTickPadding ? props.xTickPadding : 4)
      .tickFormat(props.xTickFormat ? props.xTickFormat : d3.format('d')),
    y: d3.axisLeft(scales.y)
      .ticks(props.yTicks)
      .tickPadding(props.yTickPadding ? props.yTickPadding : 4)
      .tickFormat(props.yTickFormat ? props.yTickFormat : d3.format('.1f'))
  }
}

/**
* Draw a background grid
**/

ChartLib.drawGrid = (elem, props, scales) => {
  const gridX = (props) => d3.axisBottom(scales.x).ticks(props.xTicks || 8);
  const gridY = (props) => d3.axisLeft(scales.y).ticks(props.yTicks || 8);

  d3.select(elem).select('.x.grid')
    .transition()
    .duration(1000)
    .call(gridX(props)
      .tickSize(props.height - props.margin.top - props.margin.bottom)
      .tickFormat(''))

  d3.select(elem).select('.y.grid')
    .transition()
    .duration(1000)
    .call(gridY(props)
      .tickSize(-(props.width - props.margin.left - props.margin.right))
      .tickFormat(''))
}

/**
* Draw geom: points
**/

ChartLib.drawPoint = (elem, props, domain, scales) => {
  const color = d3.scaleOrdinal('schemePaired');
  const xKey = props.x || 'x';
  const yKey = props.y || 'y';
  const j = props.jitter || false;

  const dataKey = (d, i) => i;
  const x = (d, i) => j ? jitter(scales.x(d[xKey]), i) : scales.x(d[xKey])
  const y = (d, i) => j ? jitter(scales.y(d[yKey]), i) : scales.y(d[yKey])
  const jitter = (val, idx) => idx % 2 === 0 ?
      val + (idx % 10)/8 * 10 : val - (idx % 10)/8 * 10;

  // enter
  const g = d3.select(elem).select('.point-container');

  const points = g.selectAll('.point-group')
    .data(props.pointData, props.pointKey || dataKey)

  const pointsEnter = points.enter()
    .append('g')
    .attr('class', 'point-group')
    .on('mouseout', (d) => props.onMouseout ? props.onMouseout(d) : null)
    .on('mouseenter', (d) => props.onMouseenter ? props.onMouseenter(d) : null)

  pointsEnter.append('circle').attr('class', 'point')
  pointsEnter.append('text').attr('class', 'label').attr('stroke', '#000')

  // enter + transition
  points.merge(pointsEnter).select('.point').transition()
    .duration(1000)
    .attr('cx', x)
    .attr('cy', y)
    .attr('r', (d) => props.r ? props.r : 3)
    .attr('data-key', props.pointKey || dataKey)
    .style('fill', (d) => props.pointFill ? props.pointFill(d) : color(d.key))
    .style('stroke', (d) => props.pointStroke ? props.pointStroke(d) : color(d.key))

    if (props.pointLabels) {
      points.merge(pointsEnter).select('.label').transition()
        .duration(1000)
        .text((d) => d.label ? d.label : '')
        .attr('x', (d, i) => x(d, i) + 6)
        .attr('y', (d, i) => y(d, i) - 6)
    }

  // exit
  points.exit().remove()
}

/**
* Draw geom: waffle
**/

ChartLib.drawWaffle = (elem, props, domain, scales) => {
  const size = props.waffleSize || 10;
  const maxCol = props.maxColumn || 10;
  const levelMargin = props.levelMargin || 10;

  const colCount = (d) => props.columnCounts[ d.xLevel ];

  const x = (d, i) => {
    // x offset by virtue of x's level
    const level = scales.x(d.xLevel);
    // x offset by virtue of x's column index
    const cell = d.column * size;
    // x offset to center level columns on axis label
    const center = (((maxCol - colCount(d)) * size) + levelMargin) / 2;
    return level + cell + center;
  }

  const y = (d, i) => {
    const offset = props.height - props.margin.bottom;
    const y = (d.row + 1) * size;
    return offset - y;
  }

  const waffle = d3.select(elem).select('.waffle-container')
    .selectAll('.waffle')
    .data(props.waffleData, (d) => d[props.waffleKey] || ['waffle'])

  const waffleEnter = waffle.enter()
    .append('rect')
    .attr('class', 'waffle')
    .attr('x', (d, i) => 10 * i * Math.random() - Math.random() * 10)
    .attr('y', 0)
    .on('click', (d, i) => {
      d3.select(elem).selectAll('.waffle').classed('active', false)
      d3.select(this).classed('active', true)
      if (props.onClick) props.onClick(d, i)
    })

  waffle.merge(waffleEnter).transition()
    .duration(1000)
    .attr('x', x)
    .attr('y', y)
    .attr('width', size)
    .attr('height', size)
    .attr('fill', (d, i) => props.color(d[props.colorKey]))
    .attr('stroke-width', 1)

  waffle.exit().remove()
}

export default ChartLib;