FROM php:8.2-apache-bookworm

VOLUME /var/www/html
VOLUME /var/www/lib

ADD --chmod=0755 https://github.com/mlocati/docker-php-extension-installer/releases/latest/download/install-php-extensions /usr/local/bin/

RUN mv "$PHP_INI_DIR/php.ini-production" "$PHP_INI_DIR/php.ini"

RUN install-php-extensions gd
RUN install-php-extensions mysqli
RUN install-php-extensions zip
RUN install-php-extensions mbstring
RUN install-php-extensions @composer

VOLUME /datafeed
RUN mkdir -p /datafeed/src/logs && cd /datafeed \
    && composer require arthurkushman/php-wss \
    && composer require smi2/phpclickhouse

RUN docker-php-ext-configure pcntl --enable-pcntl \
    && docker-php-ext-install \
    pcntl
VOLUME /var/www/logs
RUN mkdir -p /var/www/logs && chown www-data:www-data /var/www/logs
RUN sed -i 's/^\s*memory_limit\s*=.*$/memory_limit = 4G/' /usr/local/etc/php/php.ini
# optional HTTP port can be changed here
# RUN sed -i 's/^Listen 80$/Listen 8080/' /etc/apache2/ports.conf
# RUN sed -i 's/VirtualHost \*:80/VirtualHost \*:8080/' /etc/apache2/sites-enabled/000-default.conf
VOLUME /cache
RUN mkdir /cache
