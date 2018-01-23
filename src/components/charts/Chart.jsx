import React from 'react';
import PropTypes from 'prop-types';
import ReactDOM from 'react-dom';
import ChartLib from './chart-lib';

export default class Chart extends React.Component {
  constructor(props) {
    super(props)
    this.state = {viewportElem: null}
  }

  componentDidMount() {
    const el = ReactDOM.findDOMNode(this);
    const viewportElem = create(el, this.props);
    this.setState({viewportElem: viewportElem})
  }

  componentDidUpdate() {
    const el = ReactDOM.findDOMNode(this);
    update(el, this.props, this.state.viewportElem);
  }

  componentWillUnmount() {
    destroy(ReactDOM.findDOMNode(this));
  }

  render() {
    return (
      <div className='chart'></div>
    );
  }
}

const create = (elem, props) => {
  const viewportElem = ChartLib.createBase(elem, props);
  update(elem, props, viewportElem);
  return viewportElem;
}

const update = (elem, props, viewportElem) => {
  let viewport, chart = {};
  ChartLib.updateBase(elem, props, viewportElem);
  ChartLib.getGeoms(props).map((geom) => {
    chart.domain = ChartLib.getDomain(geom.data, props);
    chart.scales = ChartLib.getScales(elem, props, chart.domain);
    geom.draw(elem, props, chart.domain, chart.scales)
  })
  if (props.onBrush) {
    const updateViewport = props.viewport ? props.viewport : ChartLib.viewport;
    viewport = updateViewport(elem, props, chart.domain, chart.scales);
  }
  ChartLib.updateAxes(elem, props, chart.scales, viewport, viewportElem);
  if (props.legend) ChartLib.updateLegend(elem, props);
}

const destroy = () => {}

Chart.PropTypes = {
  color: PropTypes.func,
  colorKey: PropTypes.string,
  columnCounts: PropTypes.object,
  height: PropTypes.number.isRequired,
  margin: PropTypes.shape({
    bottom: PropTypes.number.isRequired,
    left: PropTypes.number.isRequired,
    right: PropTypes.number.isRequired,
    top: PropTypes.number.isRequired,
  }).isRequired,
  maxColumns: PropTypes.number,
  onClick: PropTypes.func,
  waffleData: PropTypes.arrayOf(PropTypes.shape({
    _id: PropTypes.string,
    column: PropTypes.number.isRequired,
    row: PropTypes.number.isRequired,
    similarity: PropTypes.number.isRequired,
    xLevel: PropTypes.string.isRequired,
  })),
  waffleKey: PropTypes.string,
  width: PropTypes.number.isRequired,
  x: PropTypes.string,
  xLabel: PropTypes.string,
  xLabelRotate: PropTypes.number,
  xScale: PropTypes.string,
  yScale: PropTypes.string,
  xTickFormat: PropTypes.func,
  yLabel: PropTypes.string,
}