import React from 'react';
import SortResults from './SortResults';
import UseTypes from './UseTypes';
import ResultCount from './ResultCount';
import SimilaritySlider from './SimilaritySlider';

class Filters extends React.Component {
  render() {
    return (
      <div className='filters-container'>
        <div className='filters'>
          <ResultCount />
          <div className='filter-fields'>
            <UseTypes />
            <SimilaritySlider />
            <SortResults />
          </div>
        </div>
      </div>
    )
  }
}

export default Filters;