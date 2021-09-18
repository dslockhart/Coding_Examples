import datetime
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import logging
import csv
from logging.config import fileConfig
import boto3

logging.config.fileConfig('logging_config.ini')
from decorators.timer_dec import timer_decorator


class SpotifyETL(object):
    """Pipeline to extract spotify album info"""

    def __init__(self, **kwargs):
        self.uri = kwargs.get('band_uri')
        self.logger = logging.getLogger(__name__)
        if self.uri is None:
            self.logger.error('No band uri')
            raise ValueError

    def get_spotify_object(self) -> spotipy.Spotify:
        """Initialise spotify connection object"""
        try:
            spotify = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())
            self.logger.info('Connection built')
            return spotify
        except Exception as e:
            self.logger.error(f'Connection error: {str(e)}')

    @staticmethod
    def build_data_dictionary() -> dict:
        """Return an empty dict to populate"""
        data_dictionary = {
            'Artist': [],
            'Track name': [],
            'Album name': [],
            'Year released': []
        }
        return data_dictionary

    def get_album_name(self, spotify: spotipy.Spotify) -> tuple:
        """Return the artist name from URI"""
        album_list = []
        try:
            albums = spotify.artist_albums(self.uri, country='GB', album_type='album')
            for album in albums['items']:
                if album['name'] not in album_list:
                    album_list.append(album['name'])
                    yield album['uri'], album['name'], album['release_date'].split('-')[0], album['artists'][0]['name']
        except Exception as e:
            self.logger.error(f'Get album error: {str(e)}')

    def get_track_names(self, spotify: spotipy.Spotify) -> dict:
        """Get all track names from album"""
        data_dictionary = self.build_data_dictionary()
        album = self.get_album_name(spotify)
        for id, name, year, artist in album:
            self.logger.info(f'Processing {name}')
            tracks = spotify.album_tracks(album_id=id)
            for track in tracks['items']:
                data_dictionary['Year released'].append(year)
                data_dictionary['Track name'].append(track['name'])
                data_dictionary['Album name'].append(name)
                data_dictionary['Artist'].append(artist)
        return data_dictionary

    def write_csv(self, data_dict: dict) -> None:
        """Write a csv file with the data from the data dictionary"""
        self.logger.info("Writing CSV")
        try:
            with open('albums.csv', 'w') as file:
                header = data_dict.keys()
                writer = csv.DictWriter(file, fieldnames=header)
                writer.writeheader()
                for row in range(len(data_dict['Year released'])):
                    writer.writerow({'Artist': data_dict['Artist'][row],
                                     'Track name': data_dict['Track name'][row],
                                     'Album name': data_dict['Album name'][row],
                                     'Year released': data_dict['Year released'][row]
                                     })
            self.logger.info("CSV Written")
        except Exception as e:
            self.logger.error(f'CSV write error : {str(e)}')

    @timer_decorator
    def main(self) -> None:
        """Main pipeline"""
        spotify = self.get_spotify_object()
        data_dict = self.get_track_names(spotify)
        self.write_csv(data_dict)


class AWSPush(object):
    """Class for pushing pipeline output to AWS"""

    @timer_decorator
    def push_file(self):
        s3_resource = boto3.resource('s3', aws_access_key_id=os.environ['aws_access_key_id'],
                                     aws_secret_access_key=os.environ['aws_secret_access_key'])
        date = datetime.datetime.now()
        filename = f'{date.year}/{date.month}/{date.day}/albums.csv'
        response = s3_resource.Object(bucket_name='spotifyetl', key=filename).upload_file("/tmp/albums.csv")
        return response


if __name__ == '__main__':
    args = {'band_uri': '5M52tdBnJaKSvOpJGz8mfZ'}
    pipeline = SpotifyETL(**args)
    pipeline.main()
    aws = AWSPush()
    aws.push_file()


def lambda_handler(event, context):
    args = {'band_uri': '5M52tdBnJaKSvOpJGz8mfZ'}
    pipeline = SpotifyETL(**args)
    pipeline.main()
    aws = AWSPush()
    aws.push_file()
