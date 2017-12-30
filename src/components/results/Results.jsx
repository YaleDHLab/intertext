import React from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import Result from './Result';

class Results extends React.Component {
  render() {
    const heights = getResultHeights(this.props.results)
    return (
      <div className='results'>
        <div className='result-pair-container'>
          {this.props.results.map((result, idx) => (
            <div className='result-pair' key={idx}>
              <Result result={result} type='source' height={heights[idx]} />
              <div className='similarity-circle'>
                <div className='similarity'>
                  {toInt(result.similarity) + '%'}
                </div>
              </div>
              <Result result={result} type='target' height={heights[idx]} />
            </div>
          ))}
        </div>
      </div>
    )
  }
}

// convert a percent to an integer
const toInt = (float) => Math.round(float * 100)

// compute the heights of each result pair
const getResultHeights = (results) => {
  const heights = results.reduce((arr, result) => {
    const maxLen = Math.max(result.source_segment_ids.length,
        result.target_segment_ids.length);
    arr.push(240 + (maxLen * 10))
    return arr;
  }, [])
  return heights;
}

Results.propTypes = {
  results: PropTypes.array.isRequired,
}

const mapStateToProps = state => ({
  results: state.search.results,
})

export default connect(mapStateToProps)(Results)