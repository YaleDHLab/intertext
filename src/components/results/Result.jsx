import React from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { toggleFavorite } from '../../actions/favorite';
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
  }

  getField(field, prefix) {
    var text = this.props.result[this.props.type + '_' + field];
    prefix = prefix || '';
    return {__html: prefix + text}
  }

  favorite() {
    this.props.favorite({type: this.props.type, result: this.props.result})
  }

  compare() {

  }

  visualize() {

  }

  render() {
    const id = Object.assign([], this.props.result.match_ids)
        .sort((a,b) => a-b).join('.');
    const favs = this.props.favorites[this.props.type];
    const favClass = favs.indexOf(id) > -1 ? 'favorite active' : 'favorite'

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
            <div onClick={this.compare} className='compare'>
              <CompareIcon />
              Compare
            </div>
            <div onClick={this.favorite} className={favClass}>
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

Result.propTypes = {
  favorites: PropTypes.object.isRequired,
  favorite: PropTypes.func.isRequired,
}

const mapStateToProps = state => ({
  favorites: state.favorites,
})


const mapDispatchToProps = dispatch => ({
  favorite: (obj) => dispatch(toggleFavorite(obj))
})

export default connect(mapStateToProps, mapDispatchToProps)(Result)