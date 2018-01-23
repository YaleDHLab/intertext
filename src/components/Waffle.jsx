import React from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import Chart from './charts/Chart';
import Legend from './charts/Legend';
import Loader from './Loader';
import Result from './results/Result';
import headshot from '../assets/images/authors/default-headshot.jpg';
import { colorScale } from './charts/colors';
import { getResultHeights } from './results/Results';
import {
  hideWaffle,
  setWaffleFeature,
  getWaffleActive,
 } from '../actions/waffle';

class Waffle extends React.Component {
  render() {
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
            <Legend />
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
                <div className='headshot' style={{ backgroundImage:
                    'url(' + getImage(this.props.image) + ')' }} />
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
                    waffleKey={'_id'}
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

const getImage = (image) => {
  if (image) {
    return image.substring(0,4) === 'src/' ? image.substring(3) : image;
  } else {
    return headshot;
  }
}

const options = [
  { feature: 'author', label: 'Author' },
  { feature: 'segment_ids', label: 'Segment' },
  { feature: 'year', label: 'Year' },
];

Waffle.propTypes = {
  active: PropTypes.shape({
    _id: PropTypes.string,
    similarity: PropTypes.number.isRequired,
    source_author: PropTypes.string.isRequired,
    source_file_id: PropTypes.number.isRequired,
    source_filename: PropTypes.string.isRequired,
    source_match: PropTypes.string.isRequired,
    source_postmatch: PropTypes.string.isRequired,
    source_prematch: PropTypes.string.isRequired,
    source_segment_ids: PropTypes.arrayOf(PropTypes.number.isRequired),
    source_title: PropTypes.string.isRequired,
    source_url: PropTypes.string,
    source_year: PropTypes.string.isRequired,
    target_author: PropTypes.string.isRequired,
    target_file_id: PropTypes.number.isRequired,
    target_file_path: PropTypes.string.isRequired,
    target_filename: PropTypes.string.isRequired,
    target_match: PropTypes.string.isRequired,
    target_postmatch: PropTypes.string.isRequired,
    target_prematch: PropTypes.string.isRequired,
    target_segment_ids: PropTypes.arrayOf(PropTypes.number.isRequired),
    target_title: PropTypes.string.isRequired,
    target_url: PropTypes.string,
    target_year: PropTypes.string.isRequired,
  }),
  author: PropTypes.string.isRequired,
  closeWaffle: PropTypes.func.isRequired,
  columnCounts: PropTypes.object.isRequired,
  data: PropTypes.arrayOf(PropTypes.shape({
    _id: PropTypes.string.isRequired,
    column: PropTypes.number.isRequired,
    row: PropTypes.number.isRequired,
    similarity: PropTypes.number.isRequired,
    xLevel: PropTypes.string.isRequired,
  })).isRequired,
  feature: PropTypes.string.isRequired,
  getActive: PropTypes.func.isRequired,
  history: PropTypes.shape({
    push: PropTypes.func.isRequired,
  }),
  image: PropTypes.string,
  levelMargin: PropTypes.number,
  location: PropTypes.object,
  match: PropTypes.object,
  maxColumn: PropTypes.number,
  setFeature: PropTypes.func.isRequired,
  title: PropTypes.string.isRequired,
  type: PropTypes.string.isRequired,
  width: PropTypes.number.isRequired,
  xDomain: PropTypes.arrayOf(PropTypes.string).isRequired,
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