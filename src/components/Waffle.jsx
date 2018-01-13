import React from 'react';
import PropTypes from 'prop-types';
import Chart from './charts/Chart';
import { connect } from 'react-redux';
import Loader from './Loader';
import Result from './results/Result';
import { colors, colorScale } from './charts/colors';
import { getResultHeights } from './results/Results';
import {
  hideWaffle,
  setWaffleFeature,
  getWaffleActive,
 } from '../actions/waffle';

class Waffle extends React.Component {
  render() {
    const placeholder = 'http://localhost:7092/assets/images/authors/default_author_image.jpg';
    return (
      <div className='waffle-card-wrapper'>
        <div className='waffle-card-container'>
          <div className='waffle-card-controls'>
            <span className='label'>Visualize similar passages by:</span>
            {options.map((o, i) =>
              <Button key={i}
                active={this.props.feature}
                feature={o.feature}
                label={o.label}
                setFeature={this.props.setFeature} />
            )}
            <Legend colors={colors} />
            <div className='close-visualization-wrapper'
                onClick={this.props.closeWaffle}>
              <div className='close-visualization'/>
            </div>
          </div>
          <div className='result waffle-chart-card'>
            <div className='result-top'>
              <div className='result-title'>
                <span>All passages similar to </span>
                <span><i>{this.props.title}</i></span>
              </div>
            </div>
            <div className='result-body'>
              <div className='headshot-container'>
                <div className='headshot'
                    style={{backgroundImage: 'url(' + placeholder + ')'}} />
                <div className='headshot-label'>
                  {this.props.author}
                </div>
              </div>
              <div className='waffle-chart hide-y-axis'>
                { this.props.data.length > 0 ?
                  <Chart
                    waffleData={this.props.data}
                    width={this.props.width}
                    height={236}
                    margin={{top: 0, right: 80, bottom: 75, left: 0}}
                    color={colorCell}
                    colorKey={'similarity'}
                    columnCounts={this.props.columnCounts}
                    maxColumn={this.props.maxColumn}
                    x={this.props.feature}
                    xLabel={''}
                    xScale={'ordinal'}
                    xDomain={this.props.xDomain}
                    xTickFormat={(d) => d}
                    xLabelRotate={20}
                    yLabel={''}
                    yDomain={[1,20]}
                    waffleKey={'match_ids'}
                    onClick={this.props.getActive}/>
                  : <Loader />
                }
              </div>
            </div>
          </div>
          {this.props.active ?
              <WaffleResults {...this.props}/>
            : <span/>}
        </div>
      </div>
    )
  }
}

const colorCell = (d) => colorScale( Number(Math.round(d+'e2')+'e-2') );

const Button = (props) => {
  return (
    <div className={props.feature === props.active ?
        'waffle-button active' : 'waffle-button' }
      onClick={props.setFeature.bind(null, props.feature)}>
        {props.label}
    </div>
  )
}

const Legend = (props) => {
  return (
    <div className='chart-legend'>
      <span>50%</span>
      <div className='swatches'>
        {props.colors.map((c) => <div key={c}
          className='swatch' style={{background: c}} />)}
      </div>
      <span>100%</span>
    </div>
  )
}

const WaffleResults = (props) => {
  const height = getResultHeights([props.active])[0];
  return (
    <div className='waffle-card-result-container'>
      <div className='waffle-card-results'>
        <div className='waffle-results-left'>
          <Result key='key-source'
            type={props.type}
            result={props.active}
            height={height} />
        </div>
        <div className='waffle-results-right'>
          <Result key='key-target'
            type={props.type == 'source' ? 'target' : 'source'}
            result={props.active}
            height={height} />
        </div>
        <div className='clear-both' />
      </div>
    </div>
  )
}

const options = [
  { feature: 'author', label: 'Author' },
  { feature: 'segment_ids', label: 'Segment' },
  { feature: 'year', label: 'Year' },
];

Waffle.propTypes = {
  closeWaffle: PropTypes.func.isRequired,
  setFeature: PropTypes.func.isRequired,
  feature: PropTypes.string.isRequired,
  history: PropTypes.shape({
    push: PropTypes.func.isRequired,
  }).isRequired,
}

const mapStateToProps = state => ({
  type: state.waffle.type,
  feature: state.waffle.feature,
  author: state.waffle.author,
  title: state.waffle.title,
  image: state.waffle.image,
  data: state.waffle.data,
  columnCounts: state.waffle.columnCounts,
  maxColumn: state.waffle.maxColumn,
  width: state.waffle.width,
  xDomain: state.waffle.xDomain,
  levelMargin: state.waffle.levelMargin,
  active: state.waffle.active,
})

const mapDispatchToProps = dispatch => ({
  closeWaffle: () => dispatch(hideWaffle()),
  setFeature: (feature) => dispatch(setWaffleFeature(feature)),
  getActive: (d, i) => dispatch(getWaffleActive(d, i)),
})

export default connect(mapStateToProps, mapDispatchToProps)(Waffle)