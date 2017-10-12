'use strict';

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

/**
 * main.js
 * http://www.codrops.com
 *
 * Licensed under the MIT license.
 * http://www.opensource.org/licenses/mit-license.php
 * 
 * Copyright 2017, Codrops
 * http://www.codrops.com
 */
{
	(function () {
		var ImgItem = function () {
			function ImgItem(el) {
				_classCallCheck(this, ImgItem);

				this.DOM = {};
				this.DOM.el = el;
				this.DOM.svg = this.DOM.el.querySelector('.item__svg');
				this.DOM.path = this.DOM.svg.querySelector('path');
				this.paths = {};
				this.paths.start = this.DOM.path.getAttribute('d');
				this.paths.end = this.DOM.el.dataset.morphPath;
				this.DOM.deco = this.DOM.svg.querySelector('.item__deco');
				this.DOM.image = this.DOM.svg.querySelector('image');
				this.DOM.title = this.DOM.el.querySelector('.item__meta > .item__title');
				this.DOM.subtitle = this.DOM.el.querySelector('.item__meta > .item__subtitle');
				this.CONFIG = {
					// Defaults:
					animation: {
						path: {
							duration: this.DOM.el.dataset.animationPathDuration || 1500,
							delay: this.DOM.el.dataset.animationPathDelay || 0,
							easing: this.DOM.el.dataset.animationPathEasing || 'easeOutElastic',
							elasticity: this.DOM.el.dataset.pathElasticity || 400,
							scaleX: this.DOM.el.dataset.pathScalex || 1,
							scaleY: this.DOM.el.dataset.pathScaley || 1,
							translateX: this.DOM.el.dataset.pathTranslatex || 0,
							translateY: this.DOM.el.dataset.pathTranslatey || 0,
							rotate: this.DOM.el.dataset.pathRotate || 0
						},
						image: {
							duration: this.DOM.el.dataset.animationImageDuration || 2000,
							delay: this.DOM.el.dataset.animationImageDelay || 0,
							easing: this.DOM.el.dataset.animationImageEasing || 'easeOutElastic',
							elasticity: this.DOM.el.dataset.imageElasticity || 400,
							scaleX: this.DOM.el.dataset.imageScalex || 1.1,
							scaleY: this.DOM.el.dataset.imageScaley || 1.1,
							translateX: this.DOM.el.dataset.imageTranslatex || 0,
							translateY: this.DOM.el.dataset.imageTranslatey || 0,
							rotate: this.DOM.el.dataset.imageRotate || 0
						},
						deco: {
							duration: this.DOM.el.dataset.animationDecoDuration || 2500,
							delay: this.DOM.el.dataset.animationDecoDelay || 0,
							easing: this.DOM.el.dataset.animationDecoEasing || 'easeOutQuad',
							elasticity: this.DOM.el.dataset.decoElasticity || 400,
							scaleX: this.DOM.el.dataset.decoScalex || 0.9,
							scaleY: this.DOM.el.dataset.decoScaley || 0.9,
							translateX: this.DOM.el.dataset.decoTranslatex || 0,
							translateY: this.DOM.el.dataset.decoTranslatey || 0,
							rotate: this.DOM.el.dataset.decoRotate || 0
						}
					}
				};
				this.initEvents();
			}

			_createClass(ImgItem, [{
				key: 'initEvents',
				value: function initEvents() {
					var _this = this;

					this.mouseenterFn = function () {
						_this.mouseTimeout = setTimeout(function () {
							_this.isActive = true;
							_this.animate();
						}, 75);
					};
					this.mouseleaveFn = function () {
						clearTimeout(_this.mouseTimeout);
						if (_this.isActive) {
							_this.isActive = false;
							_this.animate();
						}
					};
					this.DOM.el.addEventListener('mouseenter', this.mouseenterFn);
					this.DOM.el.addEventListener('mouseleave', this.mouseleaveFn);
					this.DOM.el.addEventListener('touchstart', this.mouseenterFn);
					this.DOM.el.addEventListener('touchend', this.mouseleaveFn);
				}
			}, {
				key: 'getAnimeObj',
				value: function getAnimeObj(targetStr) {
					var target = this.DOM[targetStr];
					var animeOpts = {
						targets: target,
						duration: this.CONFIG.animation[targetStr].duration,
						delay: this.CONFIG.animation[targetStr].delay,
						easing: this.CONFIG.animation[targetStr].easing,
						elasticity: this.CONFIG.animation[targetStr].elasticity,
						scaleX: this.isActive ? this.CONFIG.animation[targetStr].scaleX : 1,
						scaleY: this.isActive ? this.CONFIG.animation[targetStr].scaleY : 1,
						translateX: this.isActive ? this.CONFIG.animation[targetStr].translateX : 0,
						translateY: this.isActive ? this.CONFIG.animation[targetStr].translateY : 0,
						rotate: this.isActive ? this.CONFIG.animation[targetStr].rotate : 0
					};
					if (targetStr === 'path') {
						animeOpts.d = this.isActive ? this.paths.end : this.paths.start;
					}
					anime.remove(target);
					return animeOpts;
				}
			}, {
				key: 'animate',
				value: function animate() {
					// Animate the path, the image and deco.
					anime(this.getAnimeObj('path'));
					anime(this.getAnimeObj('image'));
					anime(this.getAnimeObj('deco'));
					// Title and Subtitle animation
					anime.remove(this.DOM.title);
					anime({
						targets: this.DOM.title,
						easing: 'easeOutQuad',
						translateY: this.isActive ? [{ value: '-50%', duration: 200 }, { value: ['50%', '0%'], duration: 200 }] : [{ value: '50%', duration: 200 }, { value: ['-50%', '0%'], duration: 200 }],
						opacity: [{ value: 0, duration: 200 }, { value: 1, duration: 200 }]
					});
					anime.remove(this.DOM.subtitle);
					anime({
						targets: this.DOM.subtitle,
						easing: 'easeOutQuad',
						translateY: this.isActive ? { value: ['50%', '0%'], duration: 200, delay: 250 } : { value: '0%', duration: 1 },
						opacity: this.isActive ? { value: [0, 1], duration: 200, delay: 250 } : { value: 0, duration: 1 }
					});
				}
			}]);

			return ImgItem;
		}();

		var items = Array.from(document.querySelectorAll('.item'));
		var init = function () {
			return items.forEach(function (item) {
				return new ImgItem(item);
			});
		}();
		setTimeout(function () {
			return document.body.classList.remove('loading');
		}, 2000);
	})();
};