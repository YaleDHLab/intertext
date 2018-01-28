import React from 'react';
import PropTypes from 'prop-types';
import ReactDOM from 'react-dom';
import ChartLib from './chart-lib';

export default class Chart extends React.Component {
  constructor(props) {
    super(props)
    this.state = {brush: null}
    this.create = this.create.bind(this)
    this.update = this.update.bind(this)
    this.destroy = this.destroy.bind(this)
  }

  componentDidMount() {
    const el = ReactDOM.findDOMNode(this);
    this.create(el, this.props);
  }

  componentDidUpdate(prevProps) {
    const el = ReactDOM.findDOMNode(this);
    this.update(el, this.props, this.state.brushElem);
  }

  componentWillUnmount() {
    this.destroy(ReactDOM.findDOMNode(this));
  }

  create(elem, props) {
    const brushElem = ChartLib.createBase(elem, props)
    this.update(elem, props, brushElem)
    this.setState({brushElem: brushElem});
  }

  update(elem, props, brushElem) {
    let brush, domain, scales;
    ChartLib.updateBase(elem, props)
    ChartLib.getGeoms(props).map((geom) => {
      domain = ChartLib.getDomain(geom.data, props)
      scales = ChartLib.getScales(elem, props, domain)
      geom.draw(elem, props, domain, scales)
    })
    if (props.onBrush && props.setBrush) {
      brush = props.setBrush(brushElem, scales)
    }
    ChartLib.updateAxes(elem, props, scales, brush, brushElem)
    if (props.legend) ChartLib.updateLegend(elem, props)
  }

  destroy() {}

  render() {
    return (
      <div className='chart'></div>
    );
  }
}

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
  onBrush: PropTypes.func,
  onClick: PropTypes.func,
  onMouseenter: PropTypes.func,
  onMouseout: PropTypes.func,
  onMouseover: PropTypes.func,
  setBrush: PropTypes.func,
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