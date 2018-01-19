import React from 'react';
import { connect } from 'react-redux';
import { colors } from './charts/colors';
import Chart from './charts/Chart';
import Legend from './charts/Legend';
import Loader from './Loader';
import * as d3 from 'd3';
import {
  setY,
  setUse,
  setUnit,
  resetZoom,
  setTooltip,
  setStatistic,
  toggleJitter,
  fetchScatterplotResults,
} from '../actions/scatterplot';

class Scatterplot extends React.Component {
  constructor(props) {
    super(props)
  }

  componentWillMount() {
    this.props.setUnit(getUnitFromUrl());
  }

  render() {
    const colorScale = d3.scaleQuantize()
      .domain(this.props.shownXDomain)
      .range(colors)

    return (
      <div className='scatterplot-container'>
        <div className='scatterplot hide-x-grid'>
          <h1>Popular {this.props.unit}s</h1>
          <div>
            <span>The chart below displays the most popular </span>
            <span>{this.props.unit}s </span>
            <span>within your corpus. Hover over individual points for more information.</span>
          </div>
          <Controls {...this.props} />
          <div className='left'>
            <span className='swatch-label'>Similarity</span>
            <Legend />
            <div className='jitter'>
              <span>Jitter</span>
              <input type='checkbox'
                onChange={this.props.toggleJitter}
                value={this.props.jitter} />
            </div>
            {this.props.data.length === 0 ? <Loader /> :
              <Chart
                width={480}
                height={600}
                margin={{top: 15, right: 20, bottom: 20, left: 40}}
                pointData={this.props.data}
                pointStroke={(d) => '#fff'}
                pointFill={(d) => colorScale(d.similarity)}
                pointLabels={true}
                jitter={this.props.jitter}
                r={8}
                x={'similarity'}
                xTicks={7}
                xDomain={this.props.shownXDomain}
                xTickFormat={(d) => Math.round(d * 100) / 100}
                y={this.props.y}
                yTicks={5}
                yScale={'inverse'}
                yDomain={this.props.shownYDomain}
                yTickFormat={(d) => parseInt(d)}
                drawGrid={true}
                resize={false}
                onMouseover={handleMouseover.bind(
                    this, this.props.setTooltip, this.props.y)}
                onMouseout={handleMouseout.bind(this, this.props.setTooltip)}
              />
            }
          </div>
          {this.props.tooltip.title ? <Tooltip {...this.props} /> : null}
          <Table {...this.props} />
          <div className='clear-both' />
          <div className='controls-lower'>
            <div className={this.props.zoomed ?
                'reset-button button visible' : 'reset-button button'}
              onClick={this.props.resetZoom}>Reset zoom</div>
          </div>
        </div>
        <div className='clear-both' />
      </div>
    )
  }
}

const Controls = (props) => {
  return (
    <div className='controls'>
      <span>{'Show ' + props.unit + 's most similar to'}</span>
      <select onChange={props.setUse} value={props.use}>
        <option value={'earlier'}>Earlier</option>
        <option value={'later'}>Later</option>
      </select>
      <span>{props.unit + 's based on'}</span>
      <select onChange={props.setStatistic} value={props.statistic}>
        <option value='sum'>Sum</option>
        <option value='mean'>Mean</option>
      </select>
      <span>of passage similarity</span>
    </div>
  )
}

const Tooltip = (props) => {
  return (
    <div className='tooltip' style={{
        left: props.tooltip.x + 5, top: props.tooltip.y + 30 }}>
      <div className='title'>{props.tooltip.title}</div>
      <div className='author'>
        {props.tooltip.author + ', ' + props.tooltip.year}
      </div>
    </div>
  )
}

const Table = (props) => {
  return (
    <div className='right'>
      <div className='scatterplot-label'>
        <span>Top { Math.min(20, props.data.length) } most popular </span>
        <span>{ props.unit }s </span>
        <span>in current view</span>
      </div>
      <div className='clear-both' />
      <hr />
      <table>
        <tbody>
        {props.data.slice(0,20).map((r, i) => 
          <Row key={i} unit={props.unit} row={r} />
        )}
        </tbody>
      </table>
    </div>
  )
}

const Row = (props) => {
  return (
    <tr className='book'>
      <td className='book-number'>{ props.row.label }.</td>
      <td>{getRowLabel(props)}</td>
    </tr>
  )
}

const getRowLabel = (props) => {
  switch (props.unit) {
    case 'passage':
      return '...' + props.row.match.split(' ').slice(0,20).join(' ') + '...';
    case 'author':
      return props.row.author;
    case 'book':
      return props.row.title;
  }
}

const getUnitFromUrl = () => {
  return window.location.search.substring(1).split('=')[1];
}

const handleMouseover = (setTooltip, yearField, d) => {
  const container = d3.select('.scatterplot-container').node();
  const mouseLocation = d3.mouse(container);
  setTooltip({
    x: mouseLocation[0],
    y: mouseLocation[1],
    title: d.title,
    author: d.author,
    year: d[yearField]
  })
}

const handleMouseout = (setTooltip, d) => {
  setTooltip({x: null, y: null, title: null, author: null, year: null})
}

const mapStateToProps = state => ({
  unit: state.scatterplot.unit,
  statistic: state.scatterplot.statistic,
  use: state.scatterplot.use,
  jitter: state.scatterplot.jitter,
  y: state.scatterplot.y,
  zoomed: state.scatterplot.zoom,
  data: state.scatterplot.data,
  tooltip: state.scatterplot.tooltip,
  shownXDomain: state.scatterplot.shownXDomain,
  shownYDomain: state.scatterplot.shownYDomain,
  fullXDomain: state.scatterplot.fullXDomain,
  fullYDomain: state.scatterplot.fullYDomain,
})

const mapDispatchToProps = dispatch => ({
  setY: (y) => dispatch(setY(y)),
  setUse: (e) => dispatch(setUse(e.target.value)),
  setUnit: (unit) => dispatch(setUnit(unit)),
  setStatistic: (e) => dispatch(setStatistic(e.target.value)),
  setTooltip: (obj) => dispatch(setTooltip(obj)),
  resetZoom: () => dispatch(resetZoom()),
  toggleJitter: () => dispatch(toggleJitter()),
  fetchScatterplotResults: () => dispatch(fetchScatterplotResults()),
})

export default connect(mapStateToProps, mapDispatchToProps)(Scatterplot);