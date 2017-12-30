import React from 'react';
import CompareIcon from './icons/CompareIcon';
import FavoriteIcon from './icons/FavoriteIcon';
import ReadIcon from './icons/ReadIcon';
import VisualizeIcon from './icons/VisualizeIcon';

class Result extends React.Component {
  constructor(props) {
    super(props);
    this.getTitle = this.getTitle.bind(this)
    this.getYear = this.getYear.bind(this)
    this.getAuthor = this.getAuthor.bind(this)
    this.getPrematch = this.getPrematch.bind(this)
    this.getMatch = this.getMatch.bind(this)
    this.getPostmatch = this.getPostmatch.bind(this)
  }

  getTitle() {
    return {__html: this.props.result[this.props.type + '_title']}
  }

  getYear() {
    return {__html: this.props.result[this.props.type + '_year']}
  }

  getAuthor() {
    return {__html: this.props.result[this.props.type + '_author']}
  }

  getPrematch() {
    return {__html: this.props.result[this.props.type + '_prematch'] + ' '}
  }

  getMatch() {
    return {__html: this.props.result[this.props.type + '_match']}
  }

  getPostmatch() {
    return {__html: ' ' + this.props.result[this.props.type + '_postmatch']}
  }

  render() {
    return (
      <div className={'result ' + this.props.type}
          style={{height: this.props.height}}>
        <div className='result-wrapper'>
          <div className='result-top'>
            <div className='result-title' dangerouslySetInnerHTML={this.getTitle()} />
            <div className='result-year-container'>
              <div className='result-year' dangerouslySetInnerHTML={this.getYear()} />
            </div>
          </div>
          <div className='result-body'>
            <div className='result-author' dangerouslySetInnerHTML={this.getAuthor()} />
            <div className='result-match'>
              <span className='prematch' dangerouslySetInnerHTML={this.getPrematch()} />
              <span className='match' dangerouslySetInnerHTML={this.getMatch()} />
              <span className='postmatch' dangerouslySetInnerHTML={this.getPostmatch()} />
            </div>
            <div className='white-fade' />
          </div>
        </div>
        <div className='result-footer-container'>
          <div className='result-footer'>
            <div onClick={this.read} className='read'>
              <ReadIcon />
              Read
            </div>
            <div onClick={this.compare} className='compare'>
              <CompareIcon />
              Compare
            </div>
            <div onClick={this.favorite} className='favorite'>
              <FavoriteIcon />
              Favorite
            </div>
            <div onClick={this.visualize} className='visualize'>
              <VisualizeIcon />
              Visualize
            </div>
          </div>
        </div>
      </div>
    )
  }
}

export default Result;