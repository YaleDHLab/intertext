import React from 'react';
import SortBy from './SortBy';
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
            <SimilaritySlider />
            <UseTypes />
            <SortBy />
          </div>
        </div>
      </div>
    )
  }
}

export default Filters;