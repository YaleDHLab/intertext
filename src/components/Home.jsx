import React from 'react';
import { connect } from 'react-redux';
import { Link } from 'react-router-dom';

export default class Home extends React.Component {
  render() {
    return (
      <div className='home'>
        <div className='home-top'>
          <h1>Discovering Text Reuse</h1>
          <div>Intetext helps researchers visualize text reuse in large historical collections. To explore allusions and borrowings within this corpus, click one of the images below, or type a search into the search box above.</div>
        </div>
        <div className='home-blocks'>
          <Link to='/scatterplot?unit=author' className='home-block'>
            <div className='home-image popular-authors' />
            <div>Popular Authors</div>
          </Link>
          <Link to='/scatterplot?unit=book' className='home-block'>
            <div className='home-image popular-books' />
            <div>Popular Texts</div>
          </Link>
          <Link to='/scatterplot?unit=passage' className='home-block'>
            <div className='home-image popular-passages' />
            <div>Popular Passages</div>
          </Link>
        </div>
        <div className='clear-both' />
      </div>
    )
  }
}