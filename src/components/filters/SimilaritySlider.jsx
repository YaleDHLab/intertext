import React from 'react';
import Tooltip from 'rc-tooltip';
import Slider from 'rc-slider';
import { connect } from 'react-redux';
import {
  setSimilarityAndSearch,
  setDisplayed
} from '../../actions/similarity-slider';

const Range = Slider.createSliderWithTooltip(Slider.Range);

class SimilaritySlider extends React.Component {
  constructor(props) {
    super(props)
    this.setSimilarity = this.setSimilarity.bind(this)
    this.setDisplayed = this.setDisplayed.bind(this)
  }

  setSimilarity(val) {
    this.props.setSimilarityAndSearch(val)
  }

  setDisplayed(val) {
    this.props.setDisplayed(val)
  }

  render() {
    return (
      <div className='slider'>
        <div className='filter-label'>Similarity</div>
        <div className='slider-label'>MIN</div>
          <Range
              min={50}
              max={100}
              step={1}
              value={this.props.displayed}
              onChange={this.setDisplayed}
              onAfterChange={this.setSimilarity} />
        <div className='slider-label'>MAX</div>
      </div>
    )
  }
}

const mapStateToProps = state => ({
  similarity: state.similarity.similarity,
  displayed: state.similarity.displayed,
})

const mapDispatchToProps = dispatch => ({
  setSimilarityAndSearch: (val) => dispatch(setSimilarityAndSearch(val)),
  setDisplayed: (val) => dispatch(setDisplayed(val)),
})

export default connect(mapStateToProps, mapDispatchToProps)(SimilaritySlider)