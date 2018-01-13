import React from 'react';
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