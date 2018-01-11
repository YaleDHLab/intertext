import React from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { toggleFavorite, sort } from '../../actions/favorite';
import { toggleCompare } from '../../actions/compare';
import ReadIcon from './icons/ReadIcon';
import CompareIcon from './icons/CompareIcon';
import FavoriteIcon from './icons/FavoriteIcon';
import VisualizeIcon from './icons/VisualizeIcon';

class Result extends React.Component {
  constructor(props) {
    super(props);
    this.getField = this.getField.bind(this)
    this.favorite = this.favorite.bind(this)
    this.compare = this.compare.bind(this)
    this.visualize = this.visualize.bind(this)
    this.getFavoriteClass = this.getFavoriteClass.bind(this)
  }

  getField(field, prefix) {
    var text = this.props.result[this.props.type + '_' + field];
    prefix = prefix || '';
    return {__html: prefix + text}
  }

  favorite() {
    this.props.toggleFavorite({type: this.props.type, result: this.props.result})
  }

  compare() {
    const duration = 1000;
    const results = document.querySelectorAll('.result-pair');
    const container = document.querySelector('.result-pair-container');
    for (let i=0; i<results.length; i++) {
      setTimeout(animate.bind(null, results[i], true), i * 30)
    }
    setTimeout(() => {
      container.className = container.className + ' fade-out';
      this.props.toggleCompare({type: this.props.type, result: this.props.result})
    }, duration)
    setTimeout(() => {
      container.className = container.className.replace(' fade-out', '');
      for (let i=0; i<results.length; i++) { removeAnimation(results[i]) }
    }, duration)
  }

  visualize() {

  }

  getFavoriteClass() {
    const id = Object.assign([], this.props.result.match_ids)
        .sort((a,b) => a-b).join('.');
    const favs = this.props.favorites[this.props.type];
    return favs.indexOf(id) > -1 ? 'favorite active' : 'favorite';
  }

  getCompareClass() {
    const compare = this.props.compare;
    const result = this.props.result;
    const type = this.props.type;
    const segment_ids = sort(result[ type + '_segment_ids' ]).join('.');
    return (compare.type === type &&
        compare.file_id === result[ type + '_file_id' ].toString() &&
        compare.segment_ids === segment_ids) ? 'compare active' : 'compare';
  }

  render() {
    return (
      <div className={'result ' + this.props.type}
          style={{height: this.props.height}}>
        <div className='result-wrapper'>
          <div className='result-top'>
            <div className='result-title'
                dangerouslySetInnerHTML={this.getField('title')} />
            <div className='result-year-container'>
              <div className='result-year'
                  dangerouslySetInnerHTML={this.getField('year')} />
            </div>
          </div>
          <div className='result-body'>
            <div className='result-author'
                dangerouslySetInnerHTML={this.getField('author')} />
            <div className='result-match'>
              <span className='prematch'
                  dangerouslySetInnerHTML={this.getField('prematch')} />
              <span className='match'
                  dangerouslySetInnerHTML={this.getField('match', ' ')} />
              <span className='postmatch'
                  dangerouslySetInnerHTML={this.getField('postmatch', ' ')} />
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
            <div onClick={this.compare} className={this.getCompareClass()}>
              <CompareIcon />
              Compare
            </div>
            <div onClick={this.favorite} className={this.getFavoriteClass()}>
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

const animate = (elem) => {
  elem.className = elem.className + ' animated';
  const circle = elem.querySelector('.similarity-circle');
  circle.className = circle.className + ' fade-out';
}

const removeAnimation = (elem) => {
  elem.className = elem.className.replace(' animated', '');
  const circle = elem.querySelector('.similarity-circle');
  circle.className = circle.className.replace(' fade-out', '');
}

Result.propTypes = {
  favorites: PropTypes.object.isRequired,
  compare: PropTypes.object.isRequired,
  toggleFavorite: PropTypes.func.isRequired,
  toggleCompare: PropTypes.func.isRequired,
}

const mapStateToProps = state => ({
  favorites: state.favorites,
  compare: state.compare
})

const mapDispatchToProps = dispatch => ({
  toggleFavorite: (obj) => dispatch(toggleFavorite(obj)),
  toggleCompare: (obj) => dispatch(toggleCompare(obj)),
})

export default connect(mapStateToProps, mapDispatchToProps)(Result)